package cliproxy

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	"github.com/tidwall/gjson"
)

type fiveHourRestartExecutor struct {
	provider string
	calls    chan string
}

func (e *fiveHourRestartExecutor) Identifier() string { return e.provider }
func (e *fiveHourRestartExecutor) Execute(_ context.Context, auth *coreauth.Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.calls <- auth.ID + ":" + req.Model
	return cliproxyexecutor.Response{}, nil
}
func (e *fiveHourRestartExecutor) ExecuteStream(context.Context, *coreauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, nil
}
func (e *fiveHourRestartExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}
func (e *fiveHourRestartExecutor) CountTokens(context.Context, *coreauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}
func (e *fiveHourRestartExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func TestFiveHourRestartRequestUsesPinnedCredentialAndTinyPayload(t *testing.T) {
	for _, tc := range []struct {
		provider string
		model    string
	}{
		{provider: "codex", model: "gpt-5.6-luna"},
		{provider: "claude", model: "claude-haiku-4-5-20251001"},
	} {
		t.Run(tc.provider, func(t *testing.T) {
			request, options := fiveHourRestartRequest(tc.provider, tc.model, "target-auth")
			if options.Metadata[cliproxyexecutor.PinnedAuthMetadataKey] != "target-auth" || request.Model != tc.model {
				t.Fatalf("request is not pinned to target credential: %+v %+v", request, options)
			}
			if gjson.GetBytes(request.Payload, "model").String() != tc.model {
				t.Fatalf("request model = %s", request.Payload)
			}
			if tc.provider == "claude" && (gjson.GetBytes(request.Payload, "max_tokens").Int() != 1 || gjson.GetBytes(request.Payload, "messages.0.content").String() != "Hi") {
				t.Fatalf("Claude request is not minimal generation: %s", request.Payload)
			}
			if tc.provider == "codex" && gjson.GetBytes(request.Payload, "input").String() == "" {
				t.Fatalf("Codex request has no input: %s", request.Payload)
			}
		})
	}
}

func TestFiveHourRestartEnabledByDefaultAndCanBeDisabled(t *testing.T) {
	defaultConfig, err := config.ParseConfigBytes([]byte("host: localhost\n"))
	if err != nil || !defaultConfig.RestartFiveHourWindow {
		t.Fatalf("default restart setting = %v, %v", defaultConfig, err)
	}
	disabledConfig, err := config.ParseConfigBytes([]byte("restart-five-hour-window: false\n"))
	if err != nil || disabledConfig.RestartFiveHourWindow {
		t.Fatalf("disabled restart setting = %v, %v", disabledConfig, err)
	}
}

func TestFiveHourResetUsesOnlyFiveHourWindow(t *testing.T) {
	observed := time.Unix(100000, 0)
	auth := &coreauth.Auth{Provider: "codex", Quota: coreauth.QuotaState{
		ObservedAt: observed,
		Signals: map[string]string{
			"X-Codex-Primary-Window-Minutes":        "10080",
			"X-Codex-Primary-Reset-At":              "100100",
			"X-Codex-Secondary-Window-Minutes":      "300",
			"X-Codex-Secondary-Reset-After-Seconds": "60",
		},
	}}
	if reset, ok := fiveHourResetAt(auth); !ok || !reset.Equal(observed.Add(time.Minute)) {
		t.Fatalf("Codex reset = %v, %v", reset, ok)
	}
	auth.Provider = "claude"
	auth.Quota.Signals = map[string]string{
		"Anthropic-Ratelimit-Unified-5h-Reset":  "100060",
		"Anthropic-Ratelimit-Unified-7d-Status": "rejected",
	}
	if _, ok := fiveHourResetAt(auth); ok {
		t.Fatal("weekly quota rejection must suppress restart")
	}
	delete(auth.Quota.Signals, "Anthropic-Ratelimit-Unified-7d-Status")
	if reset, ok := fiveHourResetAt(auth); !ok || !reset.Equal(observed.Add(time.Minute)) {
		t.Fatalf("Claude reset = %v, %v", reset, ok)
	}
}

func TestFiveHourRestartSendsOnceForObservedReset(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	reset := now.Add(-time.Minute)
	const authID = "five-hour-restart-target"
	const model = "gpt-5.6-luna"
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { reg.UnregisterClient(authID) })

	manager := coreauth.NewManager(nil, nil, nil)
	executor := &fiveHourRestartExecutor{provider: "codex", calls: make(chan string, 2)}
	manager.RegisterExecutor(executor)
	_, err := manager.Register(context.Background(), &coreauth.Auth{
		ID: authID, Provider: "codex", Status: coreauth.StatusActive,
		Quota: coreauth.QuotaState{ObservedAt: reset.Add(-time.Hour), Signals: map[string]string{
			"X-Codex-Primary-Window-Minutes": "300",
			"X-Codex-Primary-Reset-At":       strconv.FormatInt(reset.Unix(), 10),
		}},
	})
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}
	s := &Service{cfg: &config.Config{RestartFiveHourWindow: true}, coreManager: manager}
	attempted := make(map[string]time.Time)
	concurrent := make(chan struct{}, 1)
	s.restartDueFiveHourWindows(context.Background(), now, attempted, concurrent)
	select {
	case got := <-executor.calls:
		if got != authID+":"+model {
			t.Fatalf("executed %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("restart request was not sent")
	}
	s.restartDueFiveHourWindows(context.Background(), now.Add(time.Minute), attempted, concurrent)
	if len(executor.calls) != 0 {
		t.Fatal("same window was restarted twice")
	}
}
