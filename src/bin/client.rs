//! Клиент для подписки на стриминг котировок.
//! 
//! Обеспечивает:
//! - Подключение к серверу по TCP
//! - Отправку команды STREAM
//! - Прием котировок по UDP
//! - Отправку PING для поддержания соединения

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::thread;
use std::time::Duration;

// Импортируем модули из библиотеки
use streaming_quotes::quote::StockQuote;
use streaming_quotes::protocol::ServerResponse;
use streaming_quotes::tickers;
use streaming_quotes::{PING_INTERVAL_SECS, UDP_BUFFER_SIZE};

/// Точка входа в программу клиента
fn main() -> std::io::Result<()> {
    // Парсим аргументы командной строки
    let args: Vec<String> = std::env::args().collect();
    
    if args.len() < 4 {
        print_usage(&args[0]);
        std::process::exit(1);
    }
    
    // Получаем параметры из аргументов
    let server_addr = args[1].parse::<SocketAddr>()
        .expect(" Invalid server address format. Use: 127.0.0.1:7878");
    
    let local_port = args[2].parse::<u16>()
        .expect("Invalid port number");
    
    let tickers_file = &args[3];
    
    println!(" Starting Quote Client v1.0");
    println!("  Server: {}", server_addr);
    println!(" Local UDP port: {}", local_port);
    println!(" Tickers file: {}", tickers_file);
    
    // Читаем тикеры из файла
    let tickers = match tickers::read_tickers_from_file(tickers_file) {
        Ok(t) => {
            println!(" Loaded {} tickers", t.len());
            t
        }
        Err(e) => {
            eprintln!(" Failed to read tickers file: {}", e);
            std::process::exit(1);
        }
    };
    
    let tickers_str = tickers.join(",");
    println!(" Requesting tickers: {}", tickers_str);
    
    // Создаем UDP сокет для приема котировок
    let udp_socket = UdpSocket::bind(format!("0.0.0.0:{}", local_port))?;
    let local_addr = udp_socket.local_addr()?;
    println!(" Listening for quotes on {}", local_addr);
    
    // Подключаемся к серверу по TCP
    println!(" Connecting to server at {}", server_addr);
    let mut stream = match TcpStream::connect(server_addr) {
        Ok(s) => {
            println!(" Connected to server");
            s
        }
        Err(e) => {
            eprintln!(" Failed to connect to server: {}", e);
            std::process::exit(1);
        }
    };
    
    // Отправляем команду STREAM
    let command = format!("STREAM {} {}\n", local_addr, tickers_str);
    if let Err(e) = stream.write_all(command.as_bytes()) {
        eprintln!(" Failed to send STREAM command: {}", e);
        std::process::exit(1);
    }
    println!(" STREAM command sent");
    
    // Читаем ответ сервера
    let mut reader = BufReader::new(&stream);
    let mut response = String::new();
    
    if let Err(e) = reader.read_line(&mut response) {
        eprintln!(" Failed to read server response: {}", e);
        std::process::exit(1);
    }
    
    // Обрабатываем ответ
    match ServerResponse::parse(&response) {
        ServerResponse::Ok => {
            println!(" Subscription successful!");
        }
        ServerResponse::Error(msg) => {
            eprintln!(" Server error: {}", msg);
            std::process::exit(1);
        }
    }
    
    // Запускаем поток для отправки PING
    let server_addr_clone = server_addr;
    thread::spawn(move || {
        send_pings(server_addr_clone);
    });
    println!(" PING sender started (every {}s)", PING_INTERVAL_SECS);
    
    // Основной поток - прием и вывод котировок
    println!("Waiting for quotes...");
    println!("Press Ctrl+C to stop\n");
    
    receive_quotes(udp_socket);
    
    Ok(())
}

/// Выводит справку по использованию программы
fn print_usage(program_name: &str) {
    eprintln!("Usage: {} <server_addr> <local_udp_port> <tickers_file>", program_name);
    eprintln!();
    eprintln!("Arguments:");
    eprintln!("  server_addr      - TCP address of the server (e.g., 127.0.0.1:7878)");
    eprintln!("  local_udp_port   - Local UDP port for receiving quotes");
    eprintln!("  tickers_file     - Path to file with ticker symbols");
    eprintln!();
    eprintln!("Example:");
    eprintln!("  {} 127.0.0.1:7878 9000 assets/tickers.txt", program_name);
}

/// Отправляет PING сообщения серверу
/// 
/// # Аргументы
/// * `server_addr` - Адрес сервера для отправки PING
fn send_pings(server_addr: SocketAddr) {
    // Создаем UDP сокет для отправки PING
    let socket = match UdpSocket::bind("0.0.0.0:0") {
        Ok(s) => s,
        Err(e) => {
            eprintln!(" Failed to create PING socket: {}", e);
            return;
        }
    };
    
    loop {
        thread::sleep(Duration::from_secs(PING_INTERVAL_SECS));
        
        if let Err(e) = socket.send_to(b"PING\n", server_addr) {
            eprintln!("  Failed to send PING: {}", e);
        } else {
            println!(" PING sent");
        }
    }
}

/// Принимает и выводит котировки из UDP сокета
/// 
/// # Аргументы
/// * `socket` - UDP сокет для приема котировок
fn receive_quotes(socket: UdpSocket) {
    let mut buf = [0u8; UDP_BUFFER_SIZE];
    let mut quote_counter = 0;
    
    loop {
        match socket.recv_from(&mut buf) {
            Ok((size, _src_addr)) => {  // Используем _src_addr для игнорирования неиспользуемой переменной
                if let Ok(message) = std::str::from_utf8(&buf[..size]) {
                    let trimmed = message.trim();
                    
                    // Парсим полученную котировку
                    match StockQuote::from_wire_line(trimmed) {
                        Ok(quote) => {
                            quote_counter += 1;
                            // Красиво выводим котировку
                            println!("#{:4} | {} | ${:8.2} | Vol: {:6} | {}", 
                                quote_counter,
                                quote.ticker,
                                quote.price,
                                quote.volume,
                                quote.timestamp_ms
                            );
                        }
                        Err(e) => {
                            eprintln!("  Failed to parse quote: {} (raw: {})", e, trimmed);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("UDP receive error: {}", e);
            }
        }
    }
}