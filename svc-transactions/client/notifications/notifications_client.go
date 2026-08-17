package notifications

import (
	"context"
	"fmt"
	"time"

	notificationsv1 "github.com/Muhmd95/Contracts/notifications/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	"svc-transactions/internal/transactions"
	"svc-transactions/util/logger"
)

type grpcClient struct {
	client    notificationsv1.NotificationServiceClient
	smsQueue  chan *notificationsv1.SendNotificationRequest
	pushQueue chan *notificationsv1.SendNotificationRequest
}

func NewNotificationsClient(conn grpc.ClientConnInterface) transactions.NotificationsClient {

	grpcClient := &grpcClient{
		client:    notificationsv1.NewNotificationServiceClient(conn),
		smsQueue:  make(chan *notificationsv1.SendNotificationRequest, 100), // buffer size of 100
		pushQueue: make(chan *notificationsv1.SendNotificationRequest, 100), // buffer size of 100
	}

	// background go routines to retry faild notifications
	go func() {
		for notif := range grpcClient.smsQueue {
			_, err := grpcClient.client.SendSMSNotification(context.Background(), notif)
			if err == nil {
				fmt.Println("Successfully sent SMS notification:", notif)
			} else {
				time.Sleep(60 * time.Second) // wait for 60 seconds before retrying
				grpcClient.smsQueue <- notif // re-queue the notification for retry
			}
		}
	}()
	go func() {
		for notif := range grpcClient.pushQueue {
			_, err := grpcClient.client.SendPushNotification(context.Background(), notif)
			if err == nil {
				fmt.Println("Successfully sent push notification:", notif)
			} else {
				time.Sleep(60 * time.Second)  // wait for 60 seconds before retrying
				grpcClient.pushQueue <- notif // re-queue the notification for retry
			}

		}
	}()

	return grpcClient
}

func (c *grpcClient) SendSMSNotification(ctx context.Context, req *transactions.CreateSMSNotificationRequest) (*transactions.CreateSMSNotificationResponse, error) {
	log := logger.Ctx(ctx)
	notificationReq := &notificationsv1.SendNotificationRequest{
		PhoneNumber:   req.PhoneNumber,
		Message:       req.Message,
		TransactionId: req.TransactionID,
		WalletId:      req.WalletID,
		Amount:        req.Amount,
		Balance:       req.Balance,
		CreatedAt:     timestamppb.New(req.CreatedAt),
	}
	log.Info().Str("phone_number", req.PhoneNumber).Int64("amount", req.Amount).Msg("Sending SMS notification request to NotificationsService (from grpc notifications client)")
	clientRes, err := c.client.SendSMSNotification(ctx, notificationReq)
	if err != nil {
		st, _ := status.FromError(err)
		// ok will be true only if the error is from a grpc server
		if st.Code() == codes.Unavailable || st.Code() == codes.DeadlineExceeded {
			select {
			case c.smsQueue <- notificationReq:
				log.Warn().Msg("Queued SMS notification for retry")
			default:
				log.Error().Msg("SMS retry queue is full, notification dropped")
			}
			log.Error().Err(err).Msg("Failed to call SendSMSNotification (from grpc notifications client)")
			return nil, fmt.Errorf("failed to call SendSMSNotification: %w", err)
		}
		log.Error().Err(err).Msg("Unexpected error from NotificationsService (from grpc notifications client)")
		return nil, fmt.Errorf("unexpected error from NotificationsService: %w", err)
	}

	log.Info().Str("phone_number", notificationReq.PhoneNumber).Int64("amount", notificationReq.Amount).Msg("Successfully sent SMS notification (from grpc notifications client)")
	return &transactions.CreateSMSNotificationResponse{
		NotificationID: clientRes.NotificationId,
		Success:        clientRes.Success,
	}, nil
}

func (c *grpcClient) SendPushNotification(ctx context.Context, req *transactions.CreatePushNotificationRequest) (*transactions.CreatePushNotificationResponse, error) {
	log := logger.Ctx(ctx)
	notificationReq := &notificationsv1.SendNotificationRequest{
		PhoneNumber:   req.PhoneNumber,
		Message:       req.Message,
		TransactionId: req.TransactionID,
		WalletId:      req.WalletID,
		Amount:        req.Amount,
		Balance:       req.Balance,
		CreatedAt:     timestamppb.New(req.CreatedAt),
	}
	log.Info().Str("phone_number", req.PhoneNumber).Int64("amount", req.Amount).Msg("Sending push notification request to NotificationsService (from grpc notifications client)")
	clientRes, err := c.client.SendPushNotification(ctx, notificationReq)
	if err != nil {
		st, _ := status.FromError(err)
		// ok will be true only if the error is from a grpc server
		if st.Code() == codes.Unavailable || st.Code() == codes.DeadlineExceeded {
			select {
			case c.pushQueue <- notificationReq:
				log.Warn().Msg("Queued push notification for retry")
			default:
				log.Error().Msg("Push notification retry queue is full, notification dropped")
			}
			log.Error().Err(err).Msg("Failed to call SendPushNotification (from grpc notifications client)")
			return nil, fmt.Errorf("failed to call SendPushNotification: %w", err)
		}
		log.Error().Err(err).Msg("Unexpected error from NotificationsService (from grpc notifications client)")
		return nil, fmt.Errorf("unexpected error from NotificationsService: %w", err)
	}

	log.Info().Str("phone_number", notificationReq.PhoneNumber).Int64("amount", notificationReq.Amount).Msg("Successfully sent push notification (from grpc notifications client)")
	return &transactions.CreatePushNotificationResponse{
		NotificationID: clientRes.NotificationId,
		Success:        clientRes.Success,
	}, nil
}
