use std::fs::File;
use std::io::{BufReader};
use tokio;

use serde::Deserialize;

use sqlx::{Postgres, Pool, Row};
use sqlx::postgres::{PgPoolOptions, PgRow};
use uuid::{Uuid};
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, TryStreamExt};
use serde_json::{Value, json};

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let list = load().unwrap();

    // let clients = list.iter()
    //     .filter_map(|d| d.client.clone());
    // let distinct_clients: HashSet<String> = HashSet::from_iter(clients);
    // println!("{:?}", distinct_clients);
    let db_url = "postgres://postgres:postgrespassword@localhost/postgres";
    let ds = DataStore::new(db_url).await?;
    let ds_ref = &ds;

    let flow = tokio_stream::iter(list)
        .for_each_concurrent(128, |device| async move {
            let client_id = ds_ref.find_client(device.client.clone().unwrap()).await.unwrap();
            let e = Equipment::from(device, client_id.clone());
            let eff = ds_ref.insert_equipment(e).await.unwrap();
            println!("effect: {}", eff);
        });

    flow.await;
    println!("Done!");
    Ok(())
}

fn load() -> Result<Vec<Device>, String> {
    let file = match File::open("/home/famer/Downloads/test.json") {
        Ok(file) => file,
        Err(e) => return Err(e.to_string())
    };
    let rdr = BufReader::new(file);

    let list = match serde_json::from_reader(rdr) {
        Ok(res) => res,
        Err(e) => return Err(e.to_string())
    };
    Ok(list)
}

struct DataStore {
    pool: Pool<Postgres>
}

impl DataStore {
    async fn new(url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await?;

        Ok(Self { pool })
    }

    async fn insert_client(&self, client: Client) -> Result<u64, sqlx::Error> {

        let res = sqlx::query("INSERT INTO public.clients(id, \"createdAt\", \"updatedAt\", name, comment, address) VALUES ($1, $2, $3, $4, $5, $6)")
            .bind(client.id)
            .bind(client.created_at)
            .bind(client.updated_at)
            .bind(client.name)
            .bind(client.comment)
            .bind(client.address)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected())
    }

    async fn find_client(&self, name: String) -> Result<Option<Uuid>, sqlx::Error> {
        let res: Option<PgRow> = sqlx::query("SELECT id FROM clients WHERE name = $1")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;

        match res {
            None => Ok(None),
            Some(row) => Ok(row.try_get(0)?)
        }
    }

    async fn insert_equipment(&self, equipment: Equipment) -> Result<u64, sqlx::Error> {
        let res = sqlx::query(
            r#"insert into public.equipment (id, model, "equipmentName", "sampleId", "deviceNo",
                    manufacturer, place, equipment, "createrId", comment, "clientId",
                    address, "createdAt", "updatedAt", "equipmentTypeId")
                    values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    on conflict do nothing"#)
            .bind(equipment.id)
            .bind(equipment.model)
            .bind(equipment.equipment_name)
            .bind(equipment.sample_id)
            .bind(equipment.device_no)
            .bind(equipment.manufacturer)
            .bind(equipment.place)
            .bind(equipment.equipment)
            .bind(equipment.creater_id)
            .bind(equipment.comment)
            .bind(equipment.client_id)
            .bind(equipment.address)
            .bind(equipment.created_at)
            .bind(equipment.updated_at)
            .bind(equipment.equipment_type_id)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected())
    }
}

struct Client {
    id: Uuid,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    name: String,
    comment: String,
    address: Value
}

#[derive(Debug, Deserialize)]
struct Device {
    seq: u32,
    file_date: String,
    file_no: Option<String>,
    client: Option<String>,
    address: Option<String>,
    sample_no: Option<String>,
    location: Option<String>,
    nuclide: Option<String>,
    dose: Option<String>,
    device: Option<String>,
    device_model: Option<String>,
    device_no: Option<String>,
    vendor: Option<String>,
    place: Option<String>,
    basis: Option<String>,
    equipment: Option<String>,
    item: Option<String>,
    test_date: String
}

struct Equipment {
    id: Uuid,
    model: Option<String>,
    equipment_name: Option<String>,
    sample_id: Option<String>,
    device_no: Option<String>,
    manufacturer: Option<String>,
    place: Option<String>,
    equipment: Option<String>,
    creater_id: Option<i32>,
    comment: Option<String>,
    client_id: Option<Uuid>,
    address: Value,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    equipment_type_id: Option<Uuid>
}

impl Equipment {
    fn round_bytes(bytes: &[u8]) -> [u8; 16] {
        let mut round: [u8; 16] = [0; 16];
        for (i, b) in bytes.iter().enumerate() {
            round[i % 16] ^= b
        }
        round
    }
    fn from(device: Device, client_id: Option<Uuid>) -> Self {
        let client = device.client.clone().unwrap_or("".to_string());
        let dev = device.device.clone().unwrap_or("".to_string());
        let model = device.device_model.clone().unwrap_or("".to_string());
        let dev_no = device.device_no.clone().unwrap_or("".to_string());
        let place = device.place.clone().unwrap_or("".to_string());
        let bytes = [client.as_bytes(), dev.as_bytes(), model.as_bytes(), dev.as_bytes(), place.as_bytes()].concat();

        let uuid = Uuid::from_bytes(Self::round_bytes(&bytes[..]));
        let now = Utc::now();

        Self {
            id: uuid,
            model: device.device_model,
            equipment_name: device.device,
            sample_id: device.sample_no,
            device_no: device.device_no,
            manufacturer: device.vendor,
            place: device.place,
            equipment: device.equipment,
            creater_id: None,
            comment: None,
            client_id,
            address: json!({ "detail": device.address }),
            created_at: now,
            updated_at: now,
            equipment_type_id: None,
        }
    }
}