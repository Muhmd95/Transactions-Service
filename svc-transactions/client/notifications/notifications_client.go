package notifications

import (
	"context"
	"fmt"

	notificationsv1 "github.com/Muhmd95/Contracts/notifications/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"svc-transactions/internal/transactions"
	"svc-transactions/util/logger"
)

type grpcClient struct {
	client notificationsv1.NotificationServiceClient
}

func NewNotificationsClient(conn grpc.ClientConnInterface) transactions.NotificationsClient {
	return &grpcClient{
		client: notificationsv1.NewNotificationServiceClient(conn),
	}

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
	}
	log.Info().Str("phone_number", req.PhoneNumber).Int64("amount", req.Amount).Msg("Sending SMS notification request to NotificationsService (from grpc notifications client)")
	clientRes, err := c.client.SendSMSNotification(ctx, notificationReq)
	if err != nil {
		_, ok := status.FromError(err)
		// ok will be true only if the error is from a grpc server
		if !ok {
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
	}
	log.Info().Str("phone_number", req.PhoneNumber).Int64("amount", req.Amount).Msg("Sending push notification request to NotificationsService (from grpc notifications client)")
	clientRes, err := c.client.SendPushNotification(ctx, notificationReq)
	if err != nil {
		_, ok := status.FromError(err)
		// ok will be true only if the error is from a grpc server
		if !ok {
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
