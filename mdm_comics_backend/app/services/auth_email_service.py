"""
Auth Email Service
OPT-004: Implements email sending for password reset and email verification

Uses SendGrid for transactional email delivery.
Falls back gracefully if email is not configured.
"""
import logging
from typing import Optional

from app.core.config import settings
from app.services.email_provider import SendGridProvider, SendResult

logger = logging.getLogger(__name__)

# Singleton provider instance
_email_provider: Optional[SendGridProvider] = None


def get_email_provider() -> SendGridProvider:
    """Get or create singleton email provider."""
    global _email_provider
    if _email_provider is None:
        _email_provider = SendGridProvider()
    return _email_provider


class AuthEmailService:
    """
    Service for sending authentication-related emails.

    Handles:
    - Password reset emails
    - Email verification emails
    - Account lockout notifications
    """

    def __init__(self):
        self.provider = get_email_provider()
        self.app_url = settings.APP_URL
        self.from_email = settings.SENDGRID_FROM_EMAIL
        self.from_name = settings.SENDGRID_FROM_NAME

    async def send_password_reset_email(
        self,
        to_email: str,
        reset_token: str,
        user_name: Optional[str] = None,
    ) -> SendResult:
        """
        Send password reset email with token link.

        Args:
            to_email: Recipient email address
            reset_token: Password reset token
            user_name: Optional user's name for personalization

        Returns:
            SendResult with success status
        """
        if not settings.SENDGRID_API_KEY:
            logger.warning(
                f"Password reset email requested for {to_email} but SendGrid not configured. "
                f"Token: {reset_token[:8]}... (log for dev testing)"
            )
            return SendResult(
                success=False,
                error="Email service not configured"
            )

        reset_url = f"{self.app_url}/reset-password?token={reset_token}"
        expire_minutes = settings.PASSWORD_RESET_TOKEN_MINUTES

        # If template ID is configured, use template
        if settings.SENDGRID_TRANSACTIONAL_TEMPLATE_ID:
            result = await self.provider.send_transactional(
                to_email=to_email,
                template_id=settings.SENDGRID_TRANSACTIONAL_TEMPLATE_ID,
                dynamic_data={
                    "subject": "Reset Your Password - MDM Comics",
                    "title": "Password Reset Request",
                    "preheader": "Click the link to reset your password",
                    "user_name": user_name or "there",
                    "reset_url": reset_url,
                    "expire_minutes": expire_minutes,
                    "support_email": settings.EMAIL_FROM,
                },
            )
        else:
            # Fallback: Send plain text email via SendGrid API
            result = await self._send_plain_email(
                to_email=to_email,
                subject="Reset Your Password - MDM Comics",
                html_content=self._get_password_reset_html(
                    user_name=user_name or "there",
                    reset_url=reset_url,
                    expire_minutes=expire_minutes,
                ),
            )

        if result.success:
            logger.info(f"Password reset email sent to {to_email}")
        else:
            logger.error(f"Failed to send password reset email to {to_email}: {result.error}")

        return result

    async def send_email_verification(
        self,
        to_email: str,
        verification_token: str,
        user_name: Optional[str] = None,
    ) -> SendResult:
        """
        Send email verification link.

        Args:
            to_email: Recipient email address
            verification_token: Email verification token
            user_name: Optional user's name for personalization

        Returns:
            SendResult with success status
        """
        if not settings.SENDGRID_API_KEY:
            logger.warning(
                f"Email verification requested for {to_email} but SendGrid not configured. "
                f"Token: {verification_token[:8]}... (log for dev testing)"
            )
            return SendResult(
                success=False,
                error="Email service not configured"
            )

        verify_url = f"{self.app_url}/verify-email?token={verification_token}"
        expire_hours = settings.EMAIL_VERIFICATION_TOKEN_HOURS

        if settings.SENDGRID_TRANSACTIONAL_TEMPLATE_ID:
            result = await self.provider.send_transactional(
                to_email=to_email,
                template_id=settings.SENDGRID_TRANSACTIONAL_TEMPLATE_ID,
                dynamic_data={
                    "subject": "Verify Your Email - MDM Comics",
                    "title": "Email Verification",
                    "preheader": "Click the link to verify your email address",
                    "user_name": user_name or "there",
                    "verify_url": verify_url,
                    "expire_hours": expire_hours,
                    "support_email": settings.EMAIL_FROM,
                },
            )
        else:
            result = await self._send_plain_email(
                to_email=to_email,
                subject="Verify Your Email - MDM Comics",
                html_content=self._get_email_verification_html(
                    user_name=user_name or "there",
                    verify_url=verify_url,
                    expire_hours=expire_hours,
                ),
            )

        if result.success:
            logger.info(f"Email verification sent to {to_email}")
        else:
            logger.error(f"Failed to send email verification to {to_email}: {result.error}")

        return result

    async def send_account_locked_notification(
        self,
        to_email: str,
        user_name: Optional[str] = None,
        unlock_minutes: int = 15,
    ) -> SendResult:
        """
        Notify user their account has been locked due to failed login attempts.
        """
        if not settings.SENDGRID_API_KEY:
            logger.warning(f"Account locked notification for {to_email} skipped - no email config")
            return SendResult(success=False, error="Email service not configured")

        return await self._send_plain_email(
            to_email=to_email,
            subject="Account Security Alert - MDM Comics",
            html_content=self._get_account_locked_html(
                user_name=user_name or "there",
                unlock_minutes=unlock_minutes,
            ),
        )

    async def _send_plain_email(
        self,
        to_email: str,
        subject: str,
        html_content: str,
    ) -> SendResult:
        """Send email without template using SendGrid API directly."""
        import httpx

        http = await self.provider._get_http_client()

        payload = {
            "personalizations": [{
                "to": [{"email": to_email}],
            }],
            "from": {"email": self.from_email, "name": self.from_name},
            "subject": subject,
            "content": [
                {"type": "text/html", "value": html_content},
            ],
        }

        try:
            resp = await http.post(f"{self.provider.BASE_URL}/mail/send", json=payload)

            if resp.status_code in (200, 202):
                return SendResult(
                    success=True,
                    sent_count=1,
                    message_id=resp.headers.get("X-Message-Id"),
                )
            else:
                return SendResult(success=False, error=resp.text)
        except Exception as e:
            return SendResult(success=False, error=str(e))

    def _get_password_reset_html(
        self,
        user_name: str,
        reset_url: str,
        expire_minutes: int,
    ) -> str:
        """Generate HTML for password reset email."""
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
        </head>
        <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
            <div style="background: linear-gradient(135deg, #f97316, #ea580c); padding: 30px; text-align: center; border-radius: 10px 10px 0 0;">
                <h1 style="color: white; margin: 0; font-size: 24px;">MDM Comics</h1>
            </div>
            <div style="background: #ffffff; padding: 30px; border: 1px solid #e5e7eb; border-top: none; border-radius: 0 0 10px 10px;">
                <h2 style="color: #1f2937; margin-top: 0;">Password Reset Request</h2>
                <p>Hi {user_name},</p>
                <p>We received a request to reset your password. Click the button below to create a new password:</p>
                <div style="text-align: center; margin: 30px 0;">
                    <a href="{reset_url}" style="background: #f97316; color: white; padding: 14px 28px; text-decoration: none; border-radius: 8px; font-weight: 600; display: inline-block;">Reset Password</a>
                </div>
                <p style="color: #6b7280; font-size: 14px;">This link will expire in {expire_minutes} minutes.</p>
                <p style="color: #6b7280; font-size: 14px;">If you didn't request this, you can safely ignore this email. Your password will remain unchanged.</p>
                <hr style="border: none; border-top: 1px solid #e5e7eb; margin: 30px 0;">
                <p style="color: #9ca3af; font-size: 12px; text-align: center;">
                    MDM Comics<br>
                    This is an automated message, please do not reply.
                </p>
            </div>
        </body>
        </html>
        """

    def _get_email_verification_html(
        self,
        user_name: str,
        verify_url: str,
        expire_hours: int,
    ) -> str:
        """Generate HTML for email verification."""
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
        </head>
        <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
            <div style="background: linear-gradient(135deg, #f97316, #ea580c); padding: 30px; text-align: center; border-radius: 10px 10px 0 0;">
                <h1 style="color: white; margin: 0; font-size: 24px;">MDM Comics</h1>
            </div>
            <div style="background: #ffffff; padding: 30px; border: 1px solid #e5e7eb; border-top: none; border-radius: 0 0 10px 10px;">
                <h2 style="color: #1f2937; margin-top: 0;">Verify Your Email</h2>
                <p>Hi {user_name},</p>
                <p>Thanks for signing up! Please verify your email address by clicking the button below:</p>
                <div style="text-align: center; margin: 30px 0;">
                    <a href="{verify_url}" style="background: #f97316; color: white; padding: 14px 28px; text-decoration: none; border-radius: 8px; font-weight: 600; display: inline-block;">Verify Email</a>
                </div>
                <p style="color: #6b7280; font-size: 14px;">This link will expire in {expire_hours} hours.</p>
                <p style="color: #6b7280; font-size: 14px;">If you didn't create an account, you can safely ignore this email.</p>
                <hr style="border: none; border-top: 1px solid #e5e7eb; margin: 30px 0;">
                <p style="color: #9ca3af; font-size: 12px; text-align: center;">
                    MDM Comics<br>
                    This is an automated message, please do not reply.
                </p>
            </div>
        </body>
        </html>
        """

    def _get_account_locked_html(
        self,
        user_name: str,
        unlock_minutes: int,
    ) -> str:
        """Generate HTML for account locked notification."""
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
        </head>
        <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
            <div style="background: linear-gradient(135deg, #ef4444, #dc2626); padding: 30px; text-align: center; border-radius: 10px 10px 0 0;">
                <h1 style="color: white; margin: 0; font-size: 24px;">Security Alert</h1>
            </div>
            <div style="background: #ffffff; padding: 30px; border: 1px solid #e5e7eb; border-top: none; border-radius: 0 0 10px 10px;">
                <h2 style="color: #1f2937; margin-top: 0;">Account Temporarily Locked</h2>
                <p>Hi {user_name},</p>
                <p>We've temporarily locked your MDM Comics account due to multiple failed login attempts. This is a security measure to protect your account.</p>
                <p>Your account will automatically unlock in <strong>{unlock_minutes} minutes</strong>.</p>
                <p style="color: #6b7280; font-size: 14px;">If this wasn't you, we recommend resetting your password once your account is unlocked.</p>
                <hr style="border: none; border-top: 1px solid #e5e7eb; margin: 30px 0;">
                <p style="color: #9ca3af; font-size: 12px; text-align: center;">
                    MDM Comics Security<br>
                    This is an automated message, please do not reply.
                </p>
            </div>
        </body>
        </html>
        """


# Singleton instance
auth_email_service = AuthEmailService()
