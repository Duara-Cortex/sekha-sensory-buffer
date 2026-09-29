package config

import (
	"bufio"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// serverOnlyKeys are read by systemd/sekha-embed.service, not by this program.
var serverOnlyKeys = map[string]bool{
	"SEKHA_EMBED_SERVER_BIN": true, "SEKHA_EMBED_MODEL_PATH": true, "SEKHA_EMBED_BIND": true, "SEKHA_EMBED_PORT": true,
	"SEKHA_EMBED_THREADS": true, "SEKHA_EMBED_PARALLEL": true, "SEKHA_EMBED_CTX": true, "SEKHA_EMBED_UBATCH": true,
}

// TestEmbedUnitKeysAreDocumented checks every ${VAR} in the embed unit is in .env.example.
func TestEmbedUnitKeysAreDocumented(t *testing.T) {
	unit, err := os.ReadFile("../../systemd/sekha-embed.service")
	if err != nil {
		t.Fatal(err)
	}
	example, err := os.ReadFile("../../.env.example")
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range regexp.MustCompile(`\$\{(\w+)\}`).FindAllStringSubmatch(string(unit), -1) {
		if !strings.Contains(string(example), "\n"+m[1]+"=") {
			t.Errorf("sekha-embed.service uses %s but .env.example does not define it", m[1])
		}
	}
}

// TestEnvExampleMatchesDefaults keeps .env.example honest: it must document every key the
// program reads, and the values it shows must equal the built-in defaults.
func TestEnvExampleMatchesDefaults(t *testing.T) {
	f, err := os.Open("../../.env.example")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	file := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, _ := strings.Cut(line, "=")
		if len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
			v = v[1 : len(v)-1]
		}
		file[k] = v
	}

	read := map[string]bool{}
	got, err := Load(func(k string) (string, bool) {
		read[k] = true
		v, ok := file[k]
		return v, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	for k := range read {
		if _, ok := file[k]; !ok {
			t.Errorf(".env.example does not document %s", k)
		}
	}
	for k := range file {
		if !read[k] && !serverOnlyKeys[k] {
			t.Errorf(".env.example lists %s, which nothing reads", k)
		}
	}

	want := Defaults()
	for _, p := range []struct{ a, b *regexp.Regexp }{
		{got.SeparatorPattern, want.SeparatorPattern}, {got.HeartbeatPattern, want.HeartbeatPattern}, {got.HeartbeatExclude, want.HeartbeatExclude},
	} {
		if p.a.String() != p.b.String() {
			t.Errorf(".env.example pattern %q differs from default %q", p.a, p.b)
		}
	}
	got.SeparatorPattern, got.HeartbeatPattern, got.HeartbeatExclude = nil, nil, nil
	want.SeparatorPattern, want.HeartbeatPattern, want.HeartbeatExclude = nil, nil, nil
	if !reflect.DeepEqual(got, want) {
		t.Errorf(".env.example values differ from defaults:\n got %+v\nwant %+v", got, want)
	}
}
