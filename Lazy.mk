.PHONY: weed-large-ic-pg-mqtt-json weed-ic

weed-large-ic-pg-json-mqtt:
	cd weed && go build -tags 5BytesOffset -o $@

weed-ic:
	go build -o $@