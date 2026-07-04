//! Библиотека для стриминга биржевых котировок
//! 
//! Предоставляет основные типы данных и функции для работы с котировками,
//! протоколом обмена и файлами тикеров.

pub mod quote;
pub mod protocol;
pub mod tickers;

/// Размер буфера для UDP сообщений (2 KiB)
pub const UDP_BUFFER_SIZE: usize = 2048;

/// Таймаут ожидания PING от клиента (секунды)
pub const PING_TIMEOUT_SECS: u64 = 5;

/// Интервал отправки PING клиентом (секунды)
pub const PING_INTERVAL_SECS: u64 = 2;

/// Порт по умолчанию для TCP сервера
pub const DEFAULT_TCP_PORT: u16 = 7878;

/// Максимальная длина строки для парсинга
pub const MAX_LINE_LENGTH: usize = 1024;