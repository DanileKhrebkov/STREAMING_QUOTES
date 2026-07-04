//! Модуль для работы с биржевыми котировками.
//! 
//! Содержит структуру `StockQuote` и методы для сериализации/десериализации.

use std::fmt;

/// Структура, представляющая биржевую котировку
/// 
/// # Примеры
/// 
/// ```
/// use streaming_quotes::quote::StockQuote;
/// 
/// let quote = StockQuote {
///     ticker: "AAPL".to_string(),
///     price: 150.25,
///     volume: 1000,
///     timestamp_ms: 1234567890,
/// };
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct StockQuote {
    /// Тикер акции (например, "AAPL", "TSLA")
    pub ticker: String,
    
    /// Текущая цена акции в долларах
    pub price: f64,
    
    /// Объем торгов за текущую сессию
    pub volume: u32,
    
    /// Временная метка в миллисекундах (Unix time)
    pub timestamp_ms: u64,
}

impl StockQuote {
    /// Преобразует котировку в строку для передачи по сети
    /// 
    /// Формат: `ticker|price|volume|timestamp_ms`
    /// 
    /// # Примеры
    /// 
    /// ```
    /// # use streaming_quotes::quote::StockQuote;
    /// let quote = StockQuote {
    ///     ticker: "AAPL".to_string(),
    ///     price: 150.25,
    ///     volume: 1000,
    ///     timestamp_ms: 1234567890,
    /// };
    /// assert_eq!(quote.to_wire_line(), "AAPL|150.25|1000|1234567890");
    /// ```
    pub fn to_wire_line(&self) -> String {
        format!("{}|{}|{}|{}", self.ticker, self.price, self.volume, self.timestamp_ms)
    }

    /// Парсит строку в котировку
    /// 
    /// # Аргументы
    /// * `line` - Строка в формате `ticker|price|volume|timestamp_ms`
    /// 
    /// # Ошибки
    /// Возвращает `Err(String)` если:
    /// - Количество полей не равно 4
    /// - Поле тикера пустое
    /// - Цена, объем или временная метка не являются числами
    /// 
    /// # Примеры
    /// 
    /// ```
    /// # use streaming_quotes::quote::StockQuote;
    /// let quote = StockQuote::from_wire_line("AAPL|150.25|1000|1234567890").unwrap();
    /// assert_eq!(quote.ticker, "AAPL");
    /// ```
    pub fn from_wire_line(line: &str) -> Result<Self, String> {
        let parts: Vec<&str> = line.split('|').collect();
        
        // Проверяем количество полей
        if parts.len() != 4 {
            return Err(format!("Expected 4 fields, got {}", parts.len()));
        }

        // Парсим тикер
        let ticker = parts[0].trim().to_string();
        if ticker.is_empty() {
            return Err("Ticker cannot be empty".to_string());
        }

        // Парсим цену
        let price = parts[1].trim().parse::<f64>()
            .map_err(|e| format!("Invalid price: {}", e))?;
        
        // Парсим объем
        let volume = parts[2].trim().parse::<u32>()
            .map_err(|e| format!("Invalid volume: {}", e))?;
        
        // Парсим временную метку
        let timestamp_ms = parts[3].trim().parse::<u64>()
            .map_err(|e| format!("Invalid timestamp: {}", e))?;

        Ok(StockQuote {
            ticker,
            price,
            volume,
            timestamp_ms,
        })
    }
}

impl fmt::Display for StockQuote {
    /// Реализация Display для удобного вывода
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_wire_line())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Тест: сериализация и десериализация (round-trip)
    #[test]
    fn test_quote_roundtrip() {
        let original = StockQuote {
            ticker: "AAPL".to_string(),
            price: 150.25,
            volume: 1000,
            timestamp_ms: 1234567890,
        };
        
        let wire = original.to_wire_line();
        let parsed = StockQuote::from_wire_line(&wire).unwrap();
        
        assert_eq!(original, parsed);
    }

    /// Тест: обработка ошибок при парсинге
    #[test]
    fn test_quote_parse_error() {
        // Недостаточно полей
        assert!(StockQuote::from_wire_line("AAPL|150.25|1000").is_err());
        
        // Пустой тикер
        assert!(StockQuote::from_wire_line("|150.25|1000|123").is_err());
        
        // Неверный формат цены
        assert!(StockQuote::from_wire_line("AAPL|abc|1000|123").is_err());
        
        // Неверный формат объема
        assert!(StockQuote::from_wire_line("AAPL|150.25|abc|123").is_err());
    }

    /// Тест: корректность Display
    #[test]
    fn test_quote_display() {
        let quote = StockQuote {
            ticker: "TSLA".to_string(),
            price: 250.50,
            volume: 500,
            timestamp_ms: 9876543210,
        };
        assert_eq!(format!("{}", quote), "TSLA|250.5|500|9876543210");
    }
}