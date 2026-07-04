//! Сервер стриминга котировок.
//! 
//! Обеспечивает:
//! - Прием TCP соединений для подписки
//! - Генерацию котировок в отдельном потоке
//! - Рассылку котировок по UDP подписчикам
//! - Прием PING для поддержания соединения
//! - Автоматическое удаление неактивных подписок

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use rand::Rng;
use rand::prelude::SliceRandom;

// Импортируем модули из библиотеки
use streaming_quotes::quote::StockQuote;
use streaming_quotes::protocol::{ClientMessage, ServerResponse};
use streaming_quotes::tickers;
use streaming_quotes::{PING_TIMEOUT_SECS, UDP_BUFFER_SIZE, DEFAULT_TCP_PORT};

/// Структура, представляющая подписку клиента
#[derive(Clone)]
struct Subscription {
    /// UDP адрес для отправки котировок
    udp_addr: SocketAddr,
    
    /// Список запрашиваемых тикеров
    tickers: Vec<String>,
    
    /// Время последнего полученного PING
    last_ping: Instant,
    
    /// Отправитель в канал для получения котировок
    /// (В текущей реализации не используется, но оставлен для расширяемости)
    sender: std::sync::mpsc::Sender<StockQuote>,
}

/// Точка входа в программу сервера
fn main() -> std::io::Result<()> {
    println!("🚀 Starting Quote Server v1.0");
    println!("📡 TCP port: {}", DEFAULT_TCP_PORT);
    
    // Загружаем справочник тикеров
    let available_tickers = match tickers::read_tickers_from_file("assets/tickers.txt") {
        Ok(tickers) => {
            println!("📊 Loaded {} tickers from file", tickers.len());
            tickers
        }
        Err(e) => {
            eprintln!("⚠️  Warning: Could not read tickers file: {}", e);
            eprintln!("📊 Using default tickers for testing");
            vec!["AAPL".to_string(), "TSLA".to_string(), "MSFT".to_string()]
        }
    };
    
    // Создаем UDP сокет для приема PING
    let ping_socket = UdpSocket::bind("0.0.0.0:0")?;
    let ping_port = ping_socket.local_addr()?.port();
    println!("💓 PING listener on port {}", ping_port);
    
    // Инициализируем состояние сервера
    let state = Arc::new(Mutex::new(HashMap::<SocketAddr, Subscription>::new()));
    
    // Создаем канал для передачи котировок от генератора
    let (generator_tx, generator_rx) = std::sync::mpsc::channel::<StockQuote>();
    
    // Запускаем генератор котировок
    let available_tickers_clone = available_tickers.clone();
    thread::spawn(move || {
        generate_quotes(generator_tx, available_tickers_clone);
    });
    println!("🔄 Quote generator started");
    
    // Запускаем рассыльщик котировок
    let state_clone = state.clone();
    thread::spawn(move || {
        distribute_quotes(generator_rx, state_clone);
    });
    println!("📤 Quote distributor started");
    
    // Запускаем обработчик PING
    let state_clone = state.clone();
    thread::spawn(move || {
        handle_pings(ping_socket, state_clone);
    });
    println!("💓 PING handler started");
    
    // Запускаем проверку таймаутов
    let state_clone = state.clone();
    thread::spawn(move || {
        check_timeouts(state_clone);
    });
    println!("⏰ Timeout checker started");
    
    // Запускаем TCP сервер
    let listener = TcpListener::bind(format!("0.0.0.0:{}", DEFAULT_TCP_PORT))?;
    println!("✅ Server is ready and listening");
    
    // Основной цикл обработки подключений
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let addr = stream.peer_addr().unwrap_or_else(|_| "unknown".parse().unwrap());
                println!("🔌 New TCP connection from {}", addr);
                
                let state_clone = state.clone();
                let available_tickers_clone = available_tickers.clone();
                
                // Каждое TCP соединение обрабатывается в отдельном потоке
                thread::spawn(move || {
                    handle_client(stream, state_clone, available_tickers_clone);
                });
            }
            Err(e) => {
                eprintln!("❌ Connection failed: {}", e);
            }
        }
    }
    
    Ok(())
}

/// Генерирует котировки с случайным блужданием цен
/// 
/// # Аргументы
/// * `tx` - Отправитель для передачи котировок в канал
/// * `tickers` - Список тикеров для генерации
fn generate_quotes(tx: std::sync::mpsc::Sender<StockQuote>, tickers: Vec<String>) {
    let mut rng = rand::thread_rng();
    let mut prices: HashMap<String, f64> = HashMap::new();
    
    // Инициализируем начальные цены для всех тикеров
    for ticker in &tickers {
        prices.insert(ticker.clone(), rng.gen_range(10.0..1000.0));
    }
    
    loop {
        // Пауза между генерациями
        thread::sleep(Duration::from_secs(1));
        
        // Выбираем случайный тикер для обновления
        if let Some(ticker) = tickers.choose(&mut rng) {
            // Получаем текущую цену или инициализируем новую
            let price = prices.entry(ticker.clone()).or_insert_with(|| rng.gen_range(10.0..1000.0));
            
            // Случайное блуждание цены
            let change = rng.gen_range(-5.0..5.0);
            *price = (*price + change).max(0.01); // Не допускаем отрицательной цены
            
            // Определяем объем торгов в зависимости от "крупности" тикера
            let volume = match ticker.as_str() {
                "AAPL" | "MSFT" | "TSLA" => rng.gen_range(1000..6000),
                _ => rng.gen_range(100..1100),
            };
            
            // Создаем котировку
            let quote = StockQuote {
                ticker: ticker.clone(),
                price: *price,
                volume,
                timestamp_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            };
            
            // Отправляем в канал
            if tx.send(quote).is_err() {
                // Получатель закрыт - завершаем работу
                break;
            }
        }
    }
}

/// Рассылает котировки всем подписчикам
/// 
/// # Аргументы
/// * `rx` - Получатель из канала с котировками
/// * `state` - Состояние сервера с подписками
fn distribute_quotes(
    rx: std::sync::mpsc::Receiver<StockQuote>,
    state: Arc<Mutex<HashMap<SocketAddr, Subscription>>>,
) {
    for quote in rx {
        // Собираем список активных подписок
        let subscribers: Vec<(SocketAddr, Vec<String>)> = {
            let state = state.lock().unwrap();
            state
                .values()
                .map(|sub| (sub.udp_addr, sub.tickers.clone()))
                .collect()
        };
        
        // Для каждого подписчика проверяем, нужна ли ему эта котировка
        for (udp_addr, tickers) in subscribers {
            if tickers.contains(&quote.ticker) {
                // Отправляем UDP датаграмму
                if let Ok(socket) = UdpSocket::bind("0.0.0.0:0") {
                    let message = format!("{}\n", quote.to_wire_line());
                    if let Err(e) = socket.send_to(message.as_bytes(), udp_addr) {
                        eprintln!("⚠️  Failed to send quote to {}: {}", udp_addr, e);
                    }
                }
            }
        }
    }
}

/// Обрабатывает входящие PING сообщения от клиентов
/// 
/// # Аргументы
/// * `socket` - UDP сокет для приема PING
/// * `state` - Состояние сервера с подписками
fn handle_pings(socket: UdpSocket, state: Arc<Mutex<HashMap<SocketAddr, Subscription>>>) {
    let mut buf = [0u8; UDP_BUFFER_SIZE];
    
    loop {
        match socket.recv_from(&mut buf) {
            Ok((size, src_addr)) => {
                if let Ok(message) = std::str::from_utf8(&buf[..size]) {
                    if message.trim() == "PING" {
                        // Обновляем время последнего PING
                        let mut state = state.lock().unwrap();
                        if let Some(sub) = state.get_mut(&src_addr) {
                            sub.last_ping = Instant::now();
                            println!("💓 PING received from {}", src_addr);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("❌ PING receive error: {}", e);
            }
        }
    }
}

/// Проверяет таймауты подписок и удаляет неактивные
/// 
/// # Аргументы
/// * `state` - Состояние сервера с подписками
fn check_timeouts(state: Arc<Mutex<HashMap<SocketAddr, Subscription>>>) {
    loop {
        thread::sleep(Duration::from_secs(1));
        
        let mut state = state.lock().unwrap();
        let now = Instant::now();
        
        // Находим просроченные подписки
        let to_remove: Vec<SocketAddr> = state
            .iter()
            .filter(|(_, sub)| now.duration_since(sub.last_ping) > Duration::from_secs(PING_TIMEOUT_SECS))
            .map(|(addr, _)| *addr)
            .collect();
        
        // Удаляем просроченные подписки
        for addr in to_remove {
            println!("⏰ Removing subscription for {} (timeout)", addr);
            state.remove(&addr);
        }
    }
}

/// Обрабатывает TCP подключение клиента
/// 
/// # Аргументы
/// * `stream` - TCP поток клиента
/// * `state` - Состояние сервера с подписками
/// * `available_tickers` - Список доступных тикеров
/// Обрабатывает TCP подключение клиента
/// 
/// # Аргументы
/// * `stream` - TCP поток клиента
/// * `state` - Состояние сервера с подписками
/// * `available_tickers` - Список доступных тикеров
fn handle_client(
    mut stream: std::net::TcpStream,
    state: Arc<Mutex<HashMap<SocketAddr, Subscription>>>,
    available_tickers: Vec<String>,
) {
    let addr = stream.peer_addr().unwrap();
    
    // Читаем команду от клиента
    let mut line = String::new();
    let mut reader = BufReader::new(&stream);  // неизменяемая ссылка на stream
    
    match reader.read_line(&mut line) {
        Ok(0) => {
            println!("⚠️  Client {} closed connection", addr);
            return;
        }
        Ok(_) => {
            // Парсим команду
            match ClientMessage::parse(&line) {
                Ok(ClientMessage::Stream(cmd)) => {
                    // Проверяем, что все запрошенные тикеры доступны
                    let unknown_tickers = tickers::find_unknown_tickers(&cmd.tickers, &available_tickers);
                    
                    if !unknown_tickers.is_empty() {
                        let response = ServerResponse::Error(
                            format!("unknown ticker: {}", unknown_tickers.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(","))
                        );
                        // Получаем mutable доступ к stream через reader
                        let _ = reader.get_mut().write_all(response.to_wire().as_bytes());
                        println!("❌ Client {} requested unknown tickers: {:?}", addr, unknown_tickers);
                        return;
                    }
                    
                    // Создаем подписку
                    let (tx, _rx) = std::sync::mpsc::channel::<StockQuote>();
                    
                    let subscription = Subscription {
                        udp_addr: cmd.udp_addr,
                        tickers: cmd.tickers.clone(),
                        last_ping: Instant::now(),
                        sender: tx,
                    };
                    
                    // Сохраняем подписку
                    {
                        let mut state = state.lock().unwrap();
                        state.insert(cmd.udp_addr, subscription);
                    }
                    
                    // Отправляем подтверждение через mutable доступ к stream
                    let _ = reader.get_mut().write_all(ServerResponse::Ok.to_wire().as_bytes());
                    
                    println!("✅ Subscription registered for {}: {:?}", cmd.udp_addr, cmd.tickers);
                    
                    // Ждем закрытия соединения
                    let mut keep_alive = String::new();
                    let _ = reader.read_line(&mut keep_alive);
                    
                    // Удаляем подписку при закрытии
                    {
                        let mut state = state.lock().unwrap();
                        state.remove(&cmd.udp_addr);
                        println!("🔌 Subscription removed for {}", cmd.udp_addr);
                    }
                }
                Ok(ClientMessage::Ping) => {
                    // PING ожидается только по UDP
                    let response = ServerResponse::Error("PING must be sent via UDP".to_string());
                    let _ = reader.get_mut().write_all(response.to_wire().as_bytes());
                }
                Err(e) => {
                    let response = ServerResponse::Error(format!("invalid command: {}", e));
                    let _ = reader.get_mut().write_all(response.to_wire().as_bytes());
                    println!("❌ Invalid command from {}: {}", addr, e);
                }
            }
        }
        Err(e) => {
            eprintln!("❌ Error reading from {}: {}", addr, e);
        }
    }
}