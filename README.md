Accumulo Erlang Client using Thrift Erlang Binding

Accumulo Proxy Settings :
Since Erlang thrift binding(v0.9.1) does not support 'Compact Protocal', you must change default TCompactProtocol to TBinaryProtocol

( in $ACCUMULO/proxy/proxy.properties )

change

    protocolFactory=org.apache.thrift.protocol.TCompactProtocol$Factory
to

    protocolFactory=org.apache.thrift.protocol.TBinaryProtocol$Factory

and restart proxy server !

Compatibility :
	accumulo 1.6.0 or later
	
dependencies :

	thrift : thrift erlang binding from thrift original source
