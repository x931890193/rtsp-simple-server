package core

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/url"
	gopath "path"
	"sync"
	"time"

	"github.com/aler9/gortsplib"
	"github.com/aler9/gortsplib/pkg/rtph264"
	"github.com/asticode/go-astits"
	"github.com/grafov/m3u8"

	"github.com/aler9/rtsp-simple-server/internal/aac"
	"github.com/aler9/rtsp-simple-server/internal/h264"
	"github.com/aler9/rtsp-simple-server/internal/logger"
	"github.com/aler9/rtsp-simple-server/internal/rtcpsenderset"
)

const (
	hlsSourceRetryPause     = 5 * time.Second
	hlsSourcePauseWhenEmpty = 5 * time.Second
)

func hlsSourceURLAbsolute(base *url.URL, relative string) (*url.URL, error) {
	u, err := url.Parse(relative)
	if err != nil {
		return nil, err
	}

	if !u.IsAbs() {
		u = &url.URL{
			Scheme: base.Scheme,
			User:   base.User,
			Host:   base.Host,
			Path:   gopath.Join(gopath.Dir(base.Path), relative),
		}
	}

	return u, nil
}

type hlsSourceInstance struct {
	s *hlsSource

	ctx           context.Context
	ctxCancel     func()
	ur            *url.URL
	queue         []string
	pmtDownloaded bool
	videoPID      *uint16
	videoSPS      []byte
	videoPPS      []byte
	videoTrack    *gortsplib.Track
	videoEncoder  *rtph264.Encoder
	audioPID      *uint16
	rtcpSenders   *rtcpsenderset.RTCPSenderSet
	stream        *stream
}

func newHLSSourceInstance(
	s *hlsSource) *hlsSourceInstance {
	ctx, ctxCancel := context.WithCancel(s.ctx)

	return &hlsSourceInstance{
		s:         s,
		ctx:       ctx,
		ctxCancel: ctxCancel,
	}
}

func (si *hlsSourceInstance) close() {
	si.ctxCancel()
}

func (si *hlsSourceInstance) run() error {
	defer func() {
		if si.stream != nil {
			si.s.parent.OnSourceStaticSetNotReady(pathSourceStaticSetNotReadyReq{Source: si.s})
		}
	}()

	for {
		if len(si.queue) <= 1 {
			err := si.queueFill()
			if err != nil {
				return err
			}
		}

		if len(si.queue) == 0 {
			si.s.log(logger.Debug, "segment queue is empty, waiting")

			select {
			case <-time.After(hlsSourcePauseWhenEmpty):
			case <-si.ctx.Done():
				return fmt.Errorf("terminated")
			}
			continue
		}

		var el string
		el, si.queue = si.queue[0], si.queue[1:]

		err := si.segmentProcess(el)
		if err != nil {
			return err
		}
	}
}

func (si *hlsSourceInstance) queueFill() error {
	pl, err := func() (*m3u8.MediaPlaylist, error) {
		if si.ur == nil {
			return si.playlistDownloadMaster()
		}
		return si.playlistDownloadMedia()
	}()
	if err != nil {
		return err
	}

	for _, seg := range pl.Segments {
		if seg == nil {
			break
		}

		if !si.queueContainsURI(seg.URI) {
			si.queue = append(si.queue, seg.URI)
		}
	}

	return nil
}

func (si *hlsSourceInstance) queueContainsURI(ur string) bool {
	for _, q := range si.queue {
		if q == ur {
			return true
		}
	}
	return false
}

func (si *hlsSourceInstance) playlistDownloadMaster() (*m3u8.MediaPlaylist, error) {
	var err error
	si.ur, err = url.Parse(si.s.ur)
	if err != nil {
		return nil, err
	}

	pl, err := si.playlistDownloadSingle()
	if err != nil {
		return nil, err
	}

	switch plt := pl.(type) {
	case *m3u8.MediaPlaylist:
		return plt, nil

	case *m3u8.MasterPlaylist:
		// take the variant with the highest bandwidth
		var chosenVariant *m3u8.Variant
		for _, v := range plt.Variants {
			if chosenVariant == nil ||
				v.VariantParams.Bandwidth > chosenVariant.VariantParams.Bandwidth {
				chosenVariant = v
			}
		}

		if chosenVariant == nil {
			return nil, fmt.Errorf("no variants found")
		}

		u, err := hlsSourceURLAbsolute(si.ur, chosenVariant.URI)
		if err != nil {
			return nil, err
		}

		si.ur = u

		return si.playlistDownloadMedia()

	default:
		return nil, fmt.Errorf("invalid playlist")
	}
}

func (si *hlsSourceInstance) playlistDownloadMedia() (*m3u8.MediaPlaylist, error) {
	pl, err := si.playlistDownloadSingle()
	if err != nil {
		return nil, err
	}

	plt, ok := pl.(*m3u8.MediaPlaylist)
	if !ok {
		return nil, fmt.Errorf("invalid playlist")
	}

	return plt, nil
}

func (si *hlsSourceInstance) playlistDownloadSingle() (m3u8.Playlist, error) {
	si.s.log(logger.Debug, "downloading playlist %s", si.ur)
	req, err := http.NewRequestWithContext(si.ctx, "GET", si.ur.String(), nil)
	if err != nil {
		return nil, err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad status code: %d", res.StatusCode)
	}

	pl, _, err := m3u8.DecodeFrom(res.Body, true)
	if err != nil {
		return nil, err
	}

	return pl, nil
}

func (si *hlsSourceInstance) segmentProcess(segmentURI string) error {
	u, err := hlsSourceURLAbsolute(si.ur, segmentURI)
	if err != nil {
		return err
	}

	si.s.log(logger.Debug, "downloading segment %s", u)
	req, err := http.NewRequestWithContext(si.ctx, "GET", u.String(), nil)
	if err != nil {
		return err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status code: %d", res.StatusCode)
	}

	dem := astits.NewDemuxer(context.Background(), bufio.NewReader(res.Body))

	// get PMT
	if !si.pmtDownloaded {
		for {
			data, err := dem.NextData()
			if err != nil {
				if err == astits.ErrNoMorePackets {
					return nil
				}
				return err
			}

			if data.PMT == nil {
				continue
			}

			si.pmtDownloaded = true
			for _, e := range data.PMT.ElementaryStreams {
				switch e.StreamType {
				case astits.StreamTypeH264Video:
					if si.videoPID != nil {
						return fmt.Errorf("multiple video/audio tracks are not supported")
					}

					v := e.ElementaryPID
					si.videoPID = &v

					/*case astits.StreamTypeAACAudio:
					if si.audioPID != nil {
						return fmt.Errorf("multiple video/audio tracks are not supported")
					}

					v := e.ElementaryPID
					si.audioPID = &v*/
				}
			}
			break
		}
	}

	videoInitialized := false
	var videoStartPTS time.Duration
	var videoStartRTC time.Time

	for {
		data, err := dem.NextData()
		if err != nil {
			if err == astits.ErrNoMorePackets {
				return nil
			}
			return err
		}

		if data.PES == nil {
			continue
		}

		if si.videoPID != nil && data.PID == *si.videoPID {
			nalus, err := h264.DecodeAnnexB(data.PES.Data)
			if err != nil {
				return err
			}

			if data.PES.Header.OptionalHeader == nil ||
				data.PES.Header.OptionalHeader.PTS == nil {
				return fmt.Errorf("PTS is missing")
			}

			pts := time.Duration(float64(data.PES.Header.OptionalHeader.PTS.Base)) * time.Second / 90000
			if !videoInitialized {
				videoInitialized = true
				videoStartPTS = pts
				videoStartRTC = time.Now()
			}

			fmt.Println(pts)

			now := time.Since(videoStartRTC)
			if (pts - videoStartPTS) > now {
				select {
				case <-si.ctx.Done():
					return fmt.Errorf("terminated")
				case <-time.After(pts - videoStartPTS - now):
				}
			}

			var outNALUs [][]byte

			for _, nalu := range nalus {
				typ := h264.NALUType(nalu[0] & 0x1F)

				switch typ {
				case h264.NALUTypeSPS:
					if si.videoSPS == nil {
						si.videoSPS = append([]byte(nil), nalu...)

						if si.videoPPS != nil {
							err := si.initVideoTrack()
							if err != nil {
								return err
							}
						}
					}

					// remove since it's not needed
					continue

				case h264.NALUTypePPS:
					if si.videoPPS == nil {
						si.videoPPS = append([]byte(nil), nalu...)

						if si.videoSPS != nil {
							err := si.initVideoTrack()
							if err != nil {
								return err
							}
						}
					}

					// remove since it's not needed
					continue

				case h264.NALUTypeAccessUnitDelimiter:
					// remove since it's not needed
					continue
				}

				if si.videoEncoder == nil {
					continue
				}

				outNALUs = append(outNALUs, nalu)
			}

			if len(outNALUs) == 0 {
				continue
			}

			pkts, err := si.videoEncoder.Encode(outNALUs, pts)
			if err != nil {
				return fmt.Errorf("ERR while encoding H264: %v", err)
			}

			fmt.Println("TODO", pkts)
			/*for _, pkt := range pkts {
				si.onFrame(si.videoTrack.ID, pkt)
			}*/

		} else if si.audioPID != nil && data.PID == *si.audioPID {
			pkts, err := aac.DecodeADTS(data.PES.Data)
			if err != nil {
				return err
			}

			fmt.Println("TODO", pkts)
		}
	}
}

func (si *hlsSourceInstance) initVideoTrack() error {
	var tracks gortsplib.Tracks

	var err error
	si.videoTrack, err = gortsplib.NewTrackH264(96, si.videoSPS, si.videoPPS)
	if err != nil {
		return err
	}
	tracks = append(tracks, si.videoTrack)
	si.videoEncoder = rtph264.NewEncoder(96, nil, nil, nil)

	res := si.s.parent.OnSourceStaticSetReady(pathSourceStaticSetReadyReq{
		Tracks: tracks,
	})
	if res.Err != nil {
		return err
	}

	si.s.log(logger.Info, "ready")

	si.stream = res.Stream
	// si.rtcpSenders = rtcpsenderset.New(tracks, res.SP.OnFrame)

	return nil
}

func (si *hlsSourceInstance) onFrame(trackID int, payload []byte) {
	// si.rtcpSenders.OnFrame(trackID, gortsplib.StreamTypeRTP, payload)
	// si.sp.OnFrame(trackID, gortsplib.StreamTypeRTP, payload)
}

type hlsSourceParent interface {
	Log(logger.Level, string, ...interface{})
	OnSourceStaticSetReady(req pathSourceStaticSetReadyReq) pathSourceStaticSetReadyRes
	OnSourceStaticSetNotReady(req pathSourceStaticSetNotReadyReq)
}

type hlsSource struct {
	ur     string
	wg     *sync.WaitGroup
	parent hlsSourceParent

	ctx       context.Context
	ctxCancel func()
}

func newHLSSource(
	parentCtx context.Context,
	ur string,
	wg *sync.WaitGroup,
	parent hlsSourceParent) *hlsSource {
	ctx, ctxCancel := context.WithCancel(parentCtx)

	s := &hlsSource{
		ur:        ur,
		wg:        wg,
		parent:    parent,
		ctx:       ctx,
		ctxCancel: ctxCancel,
	}

	s.log(logger.Info, "started")

	s.wg.Add(1)
	go s.run()

	return s
}

func (s *hlsSource) Close() {
	s.log(logger.Info, "stopped")
	s.ctxCancel()
}

func (s *hlsSource) log(level logger.Level, format string, args ...interface{}) {
	s.parent.Log(level, "[hls source] "+format, args...)
}

func (s *hlsSource) run() {
	defer s.wg.Done()

outer:
	for {
		ok := s.runInner()
		if !ok {
			break outer
		}

		select {
		case <-time.After(hlsSourceRetryPause):
		case <-s.ctx.Done():
			break outer
		}
	}

	s.ctxCancel()
}

func (s *hlsSource) runInner() bool {
	si := newHLSSourceInstance(s)

	done := make(chan error)
	go func() {
		done <- si.run()
	}()

	select {
	case err := <-done:
		s.log(logger.Info, "ERR: %v", err)
		return true

	case <-s.ctx.Done():
		si.close()
		<-done
		return false
	}
}

// OnSourceAPIDescribe implements source.
func (*hlsSource) OnSourceAPIDescribe() interface{} {
	return struct {
		Type string `json:"type"`
	}{"hlsSource"}
}
