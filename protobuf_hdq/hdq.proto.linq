<Query Kind="Statements">
  <Reference Relative="..\..\..\WebApps\HdqServer\Hetco.Hdq.HdqWS\bin\Hetco.Hdq.HdqWS.dll">C:\Users\l587cj0\source\repos\FrontOfficeTools\WebApps\HdqServer\Hetco.Hdq.HdqWS\bin\Hetco.Hdq.HdqWS.dll</Reference>
  <NuGetReference>protobuf-net</NuGetReference>
</Query>

// to use protocol buffers you should start with a .proto file
// but protobuf.net generates them automatically

// this code takes the c# spec of the message and generates the .proto file contents

var protoSpec = ProtoBuf.Serializer.GetProto<HdqWS.ProtoBufDataList>();
protoSpec.Dump();