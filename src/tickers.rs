//! Модуль для работы с файлами тикеров.
//! 
//! Предоставляет функции для чтения списка тикеров из текстового файла.

use std::fs;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

/// Читает список тикеров из файла
/// 
/// # Формат файла
/// - Один тикер на строку
/// - Пустые строки игнорируются
/// - Пробелы по краям обрезаются
/// 
/// # Аргументы
/// * `path` - Путь к файлу с тикерами
/// 
/// # Возвращаемое значение
/// * `Ok(Vec<String>)` - Список тикеров
/// * `Err(io::Error)` - Ошибка чтения файла или файл пуст
/// 
/// # Примеры
/// 
/// ```no_run
/// use streaming_quotes::tickers::read_tickers_from_file;
/// 
/// let tickers = read_tickers_from_file("assets/tickers.txt").unwrap();
/// println!("Loaded {} tickers", tickers.len());
/// ```
pub fn read_tickers_from_file<P: AsRef<Path>>(path: P) -> io::Result<Vec<String>> {
    // Открываем файл
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    
    // Читаем строки, пропуская пустые
    let tickers: Vec<String> = reader
        .lines()
        .filter_map(|line| {
            let line = line.ok()?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .collect();
    
    // Проверяем, что список не пуст
    if tickers.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "No tickers found in file",
        ));
    }
    
    Ok(tickers)
}

/// Проверяет, входит ли тикер в список доступных
pub fn is_ticker_available(ticker: &str, available: &[String]) -> bool {
    available.iter().any(|t| t == ticker)
}

/// Фильтрует запрошенные тикеры, оставляя только доступные
pub fn filter_available_tickers<'a>(
    requested: &'a [String],
    available: &[String],
) -> Vec<&'a String> {
    requested
        .iter()
        .filter(|t| available.contains(t))
        .collect()
}

/// Находит неизвестные тикеры в запросе
pub fn find_unknown_tickers<'a>(
    requested: &'a [String],
    available: &[String],
) -> Vec<&'a String> {
    requested
        .iter()
        .filter(|t| !available.contains(t))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Тест: чтение тикеров из файла
    #[test]
    fn test_read_tickers_from_file() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "AAPL").unwrap();
        writeln!(temp_file, "TSLA").unwrap();
        writeln!(temp_file, "").unwrap(); // пустая строка
        writeln!(temp_file, "  MSFT  ").unwrap();
        
        let tickers = read_tickers_from_file(temp_file.path()).unwrap();
        assert_eq!(tickers, vec!["AAPL", "TSLA", "MSFT"]);
    }

    /// Тест: ошибка при пустом файле
    #[test]
    fn test_read_tickers_empty_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let result = read_tickers_from_file(temp_file.path());
        assert!(result.is_err());
    }

    /// Тест: проверка доступности тикеров
    #[test]
    fn test_ticker_availability() {
        let available = vec!["AAPL".to_string(), "TSLA".to_string()];
        
        assert!(is_ticker_available("AAPL", &available));
        assert!(!is_ticker_available("GOOG", &available));
    }

    /// Тест: фильтрация тикеров
    #[test]
    fn test_filter_available_tickers() {
        let available = vec!["AAPL".to_string(), "TSLA".to_string(), "MSFT".to_string()];
        let requested = vec!["AAPL".to_string(), "GOOG".to_string(), "MSFT".to_string()];
        
        let filtered = filter_available_tickers(&requested, &available);
        assert_eq!(filtered, vec![&"AAPL".to_string(), &"MSFT".to_string()]);
    }

    /// Тест: поиск неизвестных тикеров
    #[test]
    fn test_find_unknown_tickers() {
        let available = vec!["AAPL".to_string(), "TSLA".to_string()];
        let requested = vec!["AAPL".to_string(), "GOOG".to_string(), "MSFT".to_string()];
        
        let unknown = find_unknown_tickers(&requested, &available);
        assert_eq!(unknown, vec![&"GOOG".to_string(), &"MSFT".to_string()]);
    }
}