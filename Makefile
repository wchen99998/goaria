.PHONY: test race chaos chaos10x protocol scale profile-scale live-chaos fuzz fuzz-hour build

test:
	go test ./...

race:
	go test -race ./...

chaos:
	go test -run 'TestChaos|TestFailureRecovery|Test.*Proxy|TestNoProxy' -count=20 ./...

chaos10x:
	go test -run 'TestChaos|TestFailureRecovery|Test.*Proxy|TestNoProxy' -count=200 ./...

protocol:
	go test -run 'TestHTTP1Forced|TestHTTP2Download|TestHTTP3Download|TestHTTP3Proxy' -count=5 ./...

scale:
	go test -run TestThousandConcurrentHTTPDownloads -count=1 .

profile-scale:
	mkdir -p profiles
	go test -run '^$$' -bench BenchmarkThousandConcurrentHTTPDownloads -benchtime=5x -cpuprofile profiles/scale.cpu -memprofile profiles/scale.mem .

live-chaos:
	GOARIA_LIVE_TESTS=1 go test -run 'TestLive' -count=1 .

fuzz:
	go test -run '^$$' -fuzz=FuzzParseContentRangeTotal -fuzztime=15s .
	go test -run '^$$' -fuzz=FuzzMakeChunks -fuzztime=15s .
	go test -run '^$$' -fuzz=FuzzParseJSONRPCCall -fuzztime=15s .
	go test -run '^$$' -fuzz=FuzzProxyParsingAndBypass -fuzztime=15s .

fuzz-hour:
	go test -run '^$$' -fuzz=FuzzParseContentRangeTotal -fuzztime=15m .
	go test -run '^$$' -fuzz=FuzzMakeChunks -fuzztime=15m .
	go test -run '^$$' -fuzz=FuzzParseJSONRPCCall -fuzztime=15m .
	go test -run '^$$' -fuzz=FuzzProxyParsingAndBypass -fuzztime=15m .

build:
	go build ./cmd/goaria
