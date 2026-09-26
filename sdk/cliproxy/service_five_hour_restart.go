package cliproxy

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	log "github.com/sirupsen/logrus"
)

const fiveHourRestartPollInterval = 30 * time.Second

// runFiveHourWindowRestart observes the last generation response for each credential.
// An attempted reset is kept in memory, so failures or responses without quota
// headers cannot cause repeated requests for the same window.
func (s *Service) runFiveHourWindowRestart(ctx context.Context) {
	if s == nil || s.coreManager == nil {
		return
	}
	ticker := time.NewTicker(fiveHourRestartPollInterval)
	defer ticker.Stop()
	lastAttempt := make(map[string]time.Time)
	concurrent := make(chan struct{}, 4)
	for {
		s.restartDueFiveHourWindows(ctx, time.Now(), lastAttempt, concurrent)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) restartDueFiveHourWindows(ctx context.Context, now time.Time, lastAttempt map[string]time.Time, concurrent chan struct{}) {
	if ctx.Err() != nil {
		return
	}
	s.cfgMu.RLock()
	enabled := s.cfg != nil && s.cfg.RestartFiveHourWindow
	s.cfgMu.RUnlock()
	if !enabled {
		return
	}
	for _, auth := range s.coreManager.List() {
		resetAt, ok := fiveHourResetAt(auth)
		if !ok || now.Before(resetAt.Add(5*time.Second)) || !resetAt.After(lastAttempt[auth.ID]) {
			continue
		}
		model := fiveHourRestartModel(auth)
		if model == "" {
			continue
		}
		select {
		case concurrent <- struct{}{}:
			lastAttempt[auth.ID] = resetAt
			go func(authID, provider, model string) {
				defer func() { <-concurrent }()
				request, options := fiveHourRestartRequest(provider, model, authID)
				if _, err := s.coreManager.Execute(ctx, []string{provider}, request, options); err != nil && ctx.Err() == nil {
					log.WithError(err).WithFields(log.Fields{"provider": provider, "auth_id": authID}).Warn("five-hour window restart request failed")
				}
			}(auth.ID, auth.Provider, model)
		default:
			return
		}
	}
}

func fiveHourResetAt(auth *coreauth.Auth) (time.Time, bool) {
	if auth == nil || auth.Disabled || auth.Quota.ObservedAt.IsZero() {
		return time.Time{}, false
	}
	signals := auth.Quota.Signals
	switch auth.Provider {
	case "claude":
		if strings.EqualFold(signals["Anthropic-Ratelimit-Unified-7d-Status"], "rejected") ||
			strings.EqualFold(signals["Anthropic-Ratelimit-Unified-7d_oi-Status"], "rejected") {
			return time.Time{}, false
		}
		return parseFiveHourResetUnix(signals["Anthropic-Ratelimit-Unified-5h-Reset"], auth.Quota.ObservedAt)
	case "codex":
		for _, window := range []string{"Primary", "Secondary"} {
			prefix := "X-Codex-" + window + "-"
			if signals[prefix+"Window-Minutes"] != "300" {
				continue
			}
			if reset, ok := parseFiveHourResetUnix(signals[prefix+"Reset-At"], auth.Quota.ObservedAt); ok {
				return reset, true
			}
			seconds, err := strconv.ParseInt(signals[prefix+"Reset-After-Seconds"], 10, 64)
			if err == nil && seconds >= 0 && seconds <= int64((5*time.Hour)/time.Second) {
				return auth.Quota.ObservedAt.Add(time.Duration(seconds) * time.Second), true
			}
		}
	}
	return time.Time{}, false
}

func parseFiveHourResetUnix(raw string, observedAt time.Time) (time.Time, bool) {
	seconds, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || seconds <= 0 {
		return time.Time{}, false
	}
	reset := time.Unix(seconds, 0)
	return reset, reset.After(observedAt)
}

func fiveHourRestartModel(auth *coreauth.Auth) string {
	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	var fallback string
	for _, model := range models {
		if model == nil || model.ID == "" {
			continue
		}
		id := strings.ToLower(model.ID)
		if strings.Contains(id, "image") || strings.Contains(id, "audio") || strings.Contains(id, "realtime") {
			continue
		}
		if fallback == "" {
			fallback = model.ID
		}
		if auth.Provider == "claude" && strings.Contains(id, "haiku") ||
			auth.Provider == "codex" && (strings.Contains(id, "luna") || strings.Contains(id, "mini")) {
			return model.ID
		}
	}
	return fallback
}

func fiveHourRestartRequest(provider, model, authID string) (cliproxyexecutor.Request, cliproxyexecutor.Options) {
	format := sdktranslator.FormatOpenAIResponse
	body := map[string]any{"model": model, "input": "Hi. Reply OK."}
	if provider == "claude" {
		format = sdktranslator.FormatClaude
		body = map[string]any{
			"model": model, "max_tokens": 1,
			"messages": []map[string]string{{"role": "user", "content": "Hi"}},
		}
	}
	payload, _ := json.Marshal(body)
	return cliproxyexecutor.Request{Model: model, Payload: payload, Format: format}, cliproxyexecutor.Options{
		SourceFormat: format,
		Metadata:     map[string]any{cliproxyexecutor.PinnedAuthMetadataKey: authID},
	}
}
