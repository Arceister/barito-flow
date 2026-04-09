package flow

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func onLimitExceededGrpc() error {
	return status.Errorf(codes.ResourceExhausted, "Bandwidth Limit Exceeded")
}

func onBadRequestGrpc(err error) error {
	return status.Errorf(codes.InvalidArgument, "%s", err.Error())
}

func onStoreErrorGrpc(err error) error {
	return status.Errorf(codes.FailedPrecondition, "%s", err.Error())
}

func onCreateTopicErrorGrpc(err error) error {
	return status.Errorf(codes.Unavailable, "%s", err.Error())
}

func onSendCreateTopicErrorGrpc(err error) error {
	return status.Errorf(codes.Unavailable, "%s", err.Error())
}
