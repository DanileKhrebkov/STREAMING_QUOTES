//! Модуль сетевого протокола.
//! 
//! Определяет команды, ответы сервера и методы их парсинга.

use std::net::SocketAddr;
use std::str::FromStr;

/// Ответы сервера на команды клиента
#[derive(Debug, PartialEq)]
pub enum ServerResponse {
    /// Успешный ответ
    Ok,
    /// Ошибка с сообщением
    Error(String),
}

impl ServerResponse {
    /// Преобразует ответ в строку для отправки
    /// 
    /// # Форматы
    /// - `OK\n` для успешного ответа
    /// - `ERR <message>\n` для ошибки
    pub fn to_wire(&self) -> String {
        match self {
            ServerResponse::Ok => "OK\n".to_string(),
            ServerResponse::Error(msg) => format!("ERR {}\n", msg),
        }
    }

    /// Парсит ответ сервера из строки
    pub fn parse(line: &str) -> Self {
        let trimmed = line.trim();
        if trimmed.starts_with("OK") {
            ServerResponse::Ok
        } else if trimmed.starts_with("ERR ") {
            let msg = trimmed[4..].to_string();
            ServerResponse::Error(msg)
        } else {
            ServerResponse::Error("Unknown response".to_string())
        }
    }
}

/// Команда STREAM для подписки на котировки
#[derive(Debug, PartialEq, Clone)]
pub struct StreamCommand {
    /// UDP адрес для получения котировок
    pub udp_addr: SocketAddr,
    /// Список запрашиваемых тикеров
    pub tickers: Vec<String>,
}

impl StreamCommand {
    /// Парсит команду STREAM
    /// 
    /// # Формат
    /// `STREAM <udp_addr> <ticker1>,<ticker2>,...`
    /// 
    /// # Примеры
    /// ```
    /// # use streaming_quotes::protocol::StreamCommand;
    /// let cmd = StreamCommand::parse("STREAM 127.0.0.1:9000 AAPL,TSLA,MSFT").unwrap();
    /// assert_eq!(cmd.tickers, vec!["AAPL", "TSLA", "MSFT"]);
    /// ```
    pub fn parse(line: &str) -> Result<Self, String> {
        let trimmed = line.trim();
        
        // Проверяем, что команда начинается с STREAM
        if !trimmed.starts_with("STREAM ") {
            return Err("Invalid command: must start with 'STREAM '".to_string());
        }

        // Убираем "STREAM " в начале
        let rest = &trimmed[7..];
        
        // Разбиваем на части по пробелам
        let parts: Vec<&str> = rest.split_whitespace().collect();
        
        if parts.len() < 2 {
            return Err("Invalid command format: missing UDP address or tickers".to_string());
        }

        // Парсим UDP адрес
        let udp_addr = SocketAddr::from_str(parts[0])
            .map_err(|_| "Invalid UDP address".to_string())?;

        // Собираем все остальные части в строку тикеров
        let tickers_str = parts[1..].join("");
        
        // Парсим список тикеров (через запятую)
        let tickers: Vec<String> = tickers_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if tickers.is_empty() {
            return Err("Empty ticker list".to_string());
        }

        Ok(StreamCommand { udp_addr, tickers })
    }
}

/// Сообщения, которые клиент может отправить серверу
#[derive(Debug, PartialEq)]
pub enum ClientMessage {
    /// PING сообщение для поддержания соединения
    Ping,
    /// Команда подписки на котировки
    Stream(StreamCommand),
}

impl ClientMessage {
    /// Парсит сообщение от клиента
    /// 
    /// Поддерживает:
    /// - `PING` - keep-alive сообщение
    /// - `STREAM ...` - команда подписки
    pub fn parse(line: &str) -> Result<Self, String> {
        let trimmed = line.trim();
        
        if trimmed == "PING" {
            return Ok(ClientMessage::Ping);
        }
        
        if trimmed.starts_with("STREAM ") {
            let cmd = StreamCommand::parse(trimmed)?;
            return Ok(ClientMessage::Stream(cmd));
        }
        
        Err("Invalid command".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    /// Тест: парсинг корректной команды STREAM
    #[test]
    fn test_stream_command_parse_valid() {
        let line = "STREAM 127.0.0.1:9000 AAPL,TSLA,MSFT";
        let cmd = StreamCommand::parse(line).unwrap();
        
        assert_eq!(cmd.udp_addr, SocketAddr::from_str("127.0.0.1:9000").unwrap());
        assert_eq!(cmd.tickers, vec!["AAPL", "TSLA", "MSFT"]);
    }

    /// Тест: парсинг команды STREAM с пробелами
    #[test]
    fn test_stream_command_parse_with_spaces() {
        let line = "STREAM 127.0.0.1:9000 AAPL, TSLA , MSFT ";
        let cmd = StreamCommand::parse(line).unwrap();
        assert_eq!(cmd.tickers, vec!["AAPL", "TSLA", "MSFT"]);
    }

    /// Тест: ошибка при пустом списке тикеров
    #[test]
    fn test_stream_command_parse_empty_tickers() {
        let line = "STREAM 127.0.0.1:9000 ";
        let result = StreamCommand::parse(line);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Empty ticker list");
    }

    /// Тест: ошибка при неверном адресе
    #[test]
    fn test_stream_command_parse_invalid_address() {
        let line = "STREAM invalid:9000 AAPL";
        let result = StreamCommand::parse(line);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Invalid UDP address");
    }

    /// Тест: парсинг PING сообщения
    #[test]
    fn test_client_message_parse_ping() {
        let msg = ClientMessage::parse("PING").unwrap();
        assert_eq!(msg, ClientMessage::Ping);
    }

    /// Тест: парсинг STREAM сообщения
    #[test]
    fn test_client_message_parse_stream() {
        let msg = ClientMessage::parse("STREAM 127.0.0.1:9000 AAPL").unwrap();
        match msg {
            ClientMessage::Stream(cmd) => {
                assert_eq!(cmd.tickers, vec!["AAPL"]);
            }
            _ => panic!("Expected Stream command"),
        }
    }

    /// Тест: обработка ответов сервера
    #[test]
    fn test_server_response_parse() {
        assert_eq!(ServerResponse::parse("OK\n"), ServerResponse::Ok);
        assert_eq!(
            ServerResponse::parse("ERR invalid command\n"),
            ServerResponse::Error("invalid command".to_string())
        );
    }

    /// Тест: сериализация ответов сервера
    #[test]
    fn test_server_response_to_wire() {
        assert_eq!(ServerResponse::Ok.to_wire(), "OK\n");
        assert_eq!(
            ServerResponse::Error("test error".to_string()).to_wire(),
            "ERR test error\n"
        );
    }
}