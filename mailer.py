import smtplib
import socket
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from platform import python_version


class IPv4SMTP(smtplib.SMTP):
    def _get_socket(self, host: str, port: int, timeout: float):  # type: ignore[override]
        last_error: OSError | None = None
        for family, socktype, proto, _, sockaddr in socket.getaddrinfo(
            host,
            port,
            socket.AF_INET,
            socket.SOCK_STREAM,
        ):
            sock = None
            try:
                sock = socket.socket(family, socktype, proto)
                if timeout is not None:
                    sock.settimeout(timeout)
                if self.source_address:
                    sock.bind(self.source_address)
                sock.connect(sockaddr)
                return sock
            except OSError as exc:
                last_error = exc
                if sock is not None:
                    sock.close()

        if last_error is not None:
            raise last_error
        raise OSError(f"No IPv4 address available for {host}:{port}")


class Mailer:
    def __init__(
        self,
        sender_email: str,
        sender_password: str,
        smtp_host: str,
        smtp_port: int,
        smtp_use_tls: bool,
    ) -> None:
        self.sender_email = sender_email
        self.sender_password = sender_password
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_use_tls = smtp_use_tls

    def send_pin(self, school_login: str, pin: str) -> tuple[bool, str]:
        if not self.sender_email or not self.sender_password:
            return False, "EMAIL_NOT_CONFIGURED"

        recipient_email = f"{school_login}@student.21-school.ru"
        html = self._build_html(pin)

        msg = MIMEMultipart()
        msg["From"] = self.sender_email
        msg["To"] = recipient_email
        msg["Subject"] = "Подтверждение регистрации"
        msg["Reply-To"] = self.sender_email
        msg["Return-Path"] = self.sender_email
        msg["X-Mailer"] = "Python/" + python_version()
        msg.attach(MIMEText(html, "html"))

        try:
            self._send_message(msg, recipient_email, smtp_cls=smtplib.SMTP)
            return True, "OK"
        except OSError as exc:
            if not self._should_retry_with_ipv4(exc):
                return False, f"SMTP_NETWORK_ERROR:{exc}"

            try:
                self._send_message(msg, recipient_email, smtp_cls=IPv4SMTP)
                return True, "OK"
            except smtplib.SMTPAuthenticationError as retry_exc:
                return False, f"SMTP_AUTH_ERROR:{retry_exc.smtp_code}"
            except smtplib.SMTPException as retry_exc:
                return False, f"SMTP_ERROR:{retry_exc}"
            except OSError as retry_exc:
                return False, f"SMTP_NETWORK_ERROR:{retry_exc}"
        except smtplib.SMTPAuthenticationError as exc:
            return False, f"SMTP_AUTH_ERROR:{exc.smtp_code}"
        except smtplib.SMTPException as exc:
            return False, f"SMTP_ERROR:{exc}"

    def _send_message(
        self,
        msg: MIMEMultipart,
        recipient_email: str,
        smtp_cls: type[smtplib.SMTP],
    ) -> None:
        with smtp_cls(self.smtp_host, self.smtp_port, timeout=20) as server:
            if self.smtp_use_tls:
                server.starttls()
            server.login(self.sender_email, self.sender_password)
            server.sendmail(self.sender_email, recipient_email, msg.as_string())

    @staticmethod
    def _should_retry_with_ipv4(exc: OSError) -> bool:
        message = str(exc).lower()
        return exc.errno == 101 or "network is unreachable" in message or "timed out" in message

    @staticmethod
    def _build_html(pin: str) -> str:
        return (
            "<!DOCTYPE html>"
            "<html><head>"
            "<meta charset='UTF-8'>"
            "</head>"
            "<body style='margin:0;padding:0;font-family:Arial,sans-serif;background-color:#f9f9f9;'>"
            "<table width='100%' cellpadding='0' cellspacing='0' style='background-color:#f9f9f9;padding:20px;'>"
            "<tr><td align='center'>"
            "<table width='420px' cellpadding='0' cellspacing='0' "
            "style='background:#fff;border:2px solid black;padding:20px;text-align:center;'>"
            "<tr><td style='font-size:16px;line-height:1.5;color:#333;'>"
            "Мы получили запрос на регистрацию в сервисе поиска пиров School 21."
            "<br>Если вы не отправляли его, просто проигнорируйте это письмо."
            "</td></tr>"
            "<tr><td style='padding:10px 0;'><hr style='border:none;border-top:1px solid #ccc;'></td></tr>"
            "<tr><td style='font-size:16px;line-height:1.5;color:#333;'>"
            "Отправьте этот код в Telegram, чтобы завершить регистрацию:"
            "</td></tr>"
            f"<tr><td style='padding:20px 0;font-size:28px;font-weight:bold;color:#333;'>{pin}</td></tr>"
            "<tr><td style='font-size:14px;line-height:1.5;color:#333;'>"
            "Бот для ввода кода: "
            "<a href='https://t.me/login_school21_bot'>https://t.me/login_school21_bot</a>"
            "</td></tr>"
            "<tr><td style='font-size:14px;color:#333;'>Срок действия кода: 5 минут.</td></tr>"
            "<tr><td style='padding:10px 0;'><hr style='border:none;border-top:1px solid #ccc;'></td></tr>"
            "</table>"
            "<p style='font-size:12px;color:#666;text-align:center;margin-top:10px;'>"
            "Это письмо отправлено автоматически, отвечать на него не нужно."
            "</p></td></tr></table>"
            "</body></html>"
        )
