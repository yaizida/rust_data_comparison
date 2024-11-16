use std::collections::HashSet;
use std::env;
use tokio_postgres::{Error, NoTls};
use dotenv::dotenv;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Загружаем переменные окружения из файла .env
    dotenv().ok();

    let db_host_dev: String = env::var("DEV_DB_HOST").expect("DEV_HOST must be set");
    let db_host_prod: String = env::var("PROD_DB_HOST").expect("PROD_HOST must be set");
    let db_password: String = env::var("DB_PASSWORD").expect("Password must be set");
    let db_name: String = env::var("DB_NAME").expect("DB_NAME must be set");
    let db_user: String = env::var("DB_USER").expect("DB_USER must be set"); // Исправлено на DB_USER

    // Параметры подключения
    let dev_connection_string = format!("host={} user={} password={} dbname={}", db_host_dev, db_user, db_password, db_name);
    let prod_connection_string = format!("host={} user={} password={} dbname={}", db_host_prod, db_user, db_password, db_name);
    // Подключение к Dev базе данных
    let (dev_client, dev_connection) = tokio_postgres::connect(&dev_connection_string, NoTls).await?;
    // Подключение к Prod базе данных
    let (prod_client, prod_connection) = tokio_postgres::connect(&prod_connection_string, NoTls).await?;


    // Запускаем соединение в фоновом потоке
    tokio::spawn(async move {
        if let Err(e) = dev_connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Выполняем SELECT-запрос
    let rows = dev_client.query("SELECT schema_name, table_name FROM ods.variables_select", &[]).await?;

    tokio::spawn(async move{
        if let Err(e) = prod_connection.await{
            eprintln!("connection error: {}", e)
        }
    });


    // Обработка результатов
    for row in rows {
        let schema_name: &str = row.get(0);
        let table_name: &str = row.get(1);
        println!("schema: {}, table: {}", schema_name, table_name);
        let query_string = format!("SELECT * FROM {}.{}", schema_name, table_name);
        let prod_query_rows = prod_client.query(&query_string, &[]).await?;
        let dev_query_rows = dev_client.query(&query_string, &[]).await?;

        // Преобразуем строки в векторы для сравнения
        let prod_data: HashSet<Vec<String>> = prod_query_rows.iter()
            .map(|row| {
                (0..row.len()).map(|i| row.get::<_, &str>(i).to_string()).collect::<Vec<String>>()
            })
            .collect();

        let dev_data: HashSet<Vec<String>> = dev_query_rows.iter()
            .map(|row| {
                (0..row.len()).map(|i| row.get::<_, &str>(i).to_string()).collect::<Vec<String>>()
            })
            .collect();

        // Сравниваем данные
        let total_rows = prod_data.len() + dev_data.len();
        let matching_rows = prod_data.intersection(&dev_data).count();
        let non_matching_rows = total_rows - (2 * matching_rows);

        // Вычисляем процент расхождения
        let percentage_difference = if total_rows > 0 {
            (non_matching_rows as f64 / total_rows as f64) * 100.0
        } else {
            0.0
        };

        println!("Совпадающие строки: {}", matching_rows);
        println!("Не совпадающие строки: {}", non_matching_rows);
        println!("Процент расхождения: {:.2}%", percentage_difference);

    }

    Ok(())
}