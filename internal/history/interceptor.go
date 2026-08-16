package history

import (
	"context"
	"strings"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnaryClientInterceptor records every Relay RPC made through the conn.
// Install it outermost, so retries and leader redirects inside the client
// fall within one recorded operation's [invoke, complete] interval.
func UnaryClientInterceptor(r *Recorder) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		in, ok := describeRequest(req)
		if !ok {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		op := r.Begin(in)
		err := invoker(ctx, method, req, reply, cc, opts...)
		out := Record{Method: in.Method}
		typ := Classify(err)
		if err == nil {
			describeReply(reply, &out)
		} else {
			out.Code = status.Code(err).String()
			out.Err = status.Convert(err).Message()
		}
		r.End(op, typ, out)
		return err
	}
}

// Classify maps an RPC error to a record type:
//
//   - OK for success and for definite answers about job state (a stale
//     lease is ABORTED, an unknown job NOT_FOUND, a dead worker
//     FAILED_PRECONDITION). The op ran and its Code is the observation.
//   - Fail when the call certainly did not execute: NOT_LEADER, or a
//     request rejected as invalid.
//   - Info for everything else: unavailable, deadline exceeded, cancelled.
//     The server may or may not have applied it.
func Classify(err error) string {
	if err == nil {
		return OK
	}
	if _, _, notLeader := rpc.ParseNotLeader(err); notLeader {
		return Fail
	}
	switch status.Code(err) {
	case codes.Aborted, codes.NotFound, codes.FailedPrecondition:
		return OK
	case codes.InvalidArgument, codes.Unimplemented:
		return Fail
	}
	return Info
}

// StatusName strips the proto enum prefix: JOB_STATUS_PENDING -> PENDING.
func StatusName(s relayv1.JobStatus) string {
	return strings.TrimPrefix(s.String(), "JOB_STATUS_")
}

func describeRequest(req any) (Record, bool) {
	switch q := req.(type) {
	case *relayv1.SubmitJobRequest:
		return Record{Method: Submit, Key: q.GetIdempotencyKey(), MaxAttempts: int(q.GetMaxAttempts())}, true
	case *relayv1.GetJobRequest:
		return Record{Method: Get, JobID: q.GetJobId()}, true
	case *relayv1.RegisterWorkerRequest:
		return Record{Method: Register}, true
	case *relayv1.HeartbeatRequest:
		return Record{Method: Heartbeat, Worker: q.GetWorkerId()}, true
	case *relayv1.AcquireJobRequest:
		return Record{Method: Acquire, Worker: q.GetWorkerId()}, true
	case *relayv1.CompleteJobRequest:
		return Record{Method: Complete, JobID: q.GetJobId(), Worker: q.GetWorkerId(), Token: q.GetLeaseToken()}, true
	case *relayv1.FailJobRequest:
		return Record{Method: FailJob, JobID: q.GetJobId(), Worker: q.GetWorkerId(), Token: q.GetLeaseToken(), Retryable: q.GetRetryable()}, true
	}
	return Record{}, false
}

func describeReply(reply any, out *Record) {
	switch p := reply.(type) {
	case *relayv1.SubmitJobResponse:
		out.JobID, out.Duplicate = p.GetJobId(), p.GetDuplicate()
	case *relayv1.GetJobResponse:
		describeJob(p.GetJob(), out)
	case *relayv1.RegisterWorkerResponse:
		out.Worker = p.GetWorkerId()
	case *relayv1.AcquireJobResponse:
		out.HasJob = p.GetHasJob()
		if p.GetHasJob() {
			describeJob(p.GetJob(), out)
		}
	case *relayv1.CompleteJobResponse:
		out.Already = p.GetAlreadyCompleted()
	case *relayv1.FailJobResponse:
		out.Status = StatusName(p.GetStatus())
	}
}

func describeJob(j *relayv1.Job, out *Record) {
	out.JobID = j.GetId()
	out.Status = StatusName(j.GetStatus())
	out.Token = j.GetLeaseToken()
	out.Attempts = int(j.GetAttempts())
	out.MaxAttempts = int(j.GetMaxAttempts())
	out.Owner = j.GetLeaseOwner()
	out.Key = j.GetIdempotencyKey()
}
