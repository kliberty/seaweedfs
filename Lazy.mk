.PHONY: weed-large-ic-pg-mqtt-json weed-ic weed-large-ic-pg-json-mqtt-debug

weed-large-ic-pg-json-mqtt:
	cd weed && go build -tags 5BytesOffset -o $@
	
weed-large-ic-pg-json-mqtt-debug:
	cd weed && go build -tags 5BytesOffset -gcflags=all="-N -l" -o $@

weed-ic:
	go build -o $@