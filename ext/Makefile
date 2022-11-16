TOPDIR = ..
JSONTAR = json-c.tar.gz
JSONDIR = $(JSONTAR:.tar.gz=)
XRGTAR = xrg-r220920.zip
XRGDIR = $(XRGTAR:.zip=)
ARROWZIP = apache-arrow-7.0.0-static.zip
ARROWDIR = $(ARROWZIP:.zip=)
SOCKSTREAMTAR = sockstream-r220819.zip
SOCKSTREAMDIR = $(SOCKSTREAMTAR:.zip=)
HOPZIP = hop-r221018.zip
HOPDIR = $(HOPZIP:.zip=)
KITESDKZIP = kite-client-sdk-r221116.zip
KITESDKDIR = $(KITESDKZIP:.zip=)


all: $(SUBDIR) include/kitesdk.h include/arrow/api.h include/hop/hashagg.h

$(SUBDIR):
	mkdir $@

include/kitesdk.h: tarball/$(KITESDKZIP)
	rm -rf $(KITESDKDIR)
	unzip -q tarball/$(KITESDKZIP)
	make -C $(KITESDKDIR) install prefix=$(shell pwd)

include/hop/hashagg.h: tarball/$(HOPZIP)
	rm -rf $(HOPDIR)
	unzip -q tarball/$(HOPZIP)
	make -C $(HOPDIR) install prefix=$(shell pwd)

include/arrow/api.h: tarball/$(ARROWZIP)
	rm -rf $(ARROWDIR)
	unzip -q tarball/$(ARROWZIP)
	cp -r $(ARROWDIR)/include/* include
	cp -r $(ARROWDIR)/lib/* lib

include/rapidjson: tarball/rapidjson-master.zip
	rm -rf rapidjson-master
	unzip -q tarball/rapidjson-master.zip
	cp -r rapidjson-master/include/rapidjson include
	rm -rf rapidjson-master


wipe:
	rm -rf bin include lib share $(KITESDKDIR) $(ARROWDIR) $(HOPDIR)
