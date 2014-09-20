package server

import (
	"keyvalue/kvservice"

	"code.google.com/p/goprotobuf/proto"
)

type KVService struct {
	Server *Server
}

func (kv *KVService) Get(in *kvservice.GetRequest, out *kvservice.Response) error {
	status, value := kv.Server.Get(*in.Key)
	out.Result = proto.Int32(int32(status))
	out.Value = proto.String(value)
	return nil
}

func (kv *KVService) Set(in *kvservice.SetRequest, out *kvservice.Response) error {
	status, value := kv.Server.Set(*in.Key, *in.Value)
	out.Result = proto.Int32(int32(status))
	out.Value = proto.String(value)
	return nil
}
