use std::env;
use tokio_postgres::{NoTls, Error};
use dotenv::dotenv;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Загружаем переменные окружения из файла .env
    dotenv().ok();

    let db_host: String = env::var("DEV_DB_HOST").expect("HOST must be set");
    let db_password: String = env::var("DB_PASSWORD").expect("Password must be set");
    let db_name: String = env::var("DB_NAME").expect("DB_NAME must be set");
    let db_user: String = env::var("DB_USER").expect("DB_USER must be set"); // Исправлено на DB_USER

    // Параметры подключения
    let connection_string = format!("host={} user={} password={} dbname={}", db_host, db_user, db_password, db_name);

    // Подключение к базе данных
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls).await?;

    // Запускаем соединение в фоновом потоке
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Выполняем SELECT-запрос
    let rows = client.query("SELECT schema_name, table_name FROM ods.variables_select", &[]).await?;

    // Обработка результатов
    for row in rows {
        let schema_name: &str = row.get(0);
        let table_name: &str = row.get(1);
        println!("schema: {}, table: {}", schema_name, table_name);
    }

    Ok(())
}