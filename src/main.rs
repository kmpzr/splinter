use anyhow::Result;
use base64::{engine::general_purpose, Engine as _};
use futures::future::join_all;
use log::{debug, error, info, warn};
use reqwest::ClientBuilder;
use serde::{Deserialize, Serialize};
use serde_aux::prelude::*;
use serde_yaml::{self};
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time;
use tokio_postgres::types::Type;
use tokio_postgres::NoTls;

static DB_SCHEMA: &str = "splinter";
static APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

#[derive(Serialize, Deserialize, Debug)]
struct Config {
  pipelines: Vec<PipelineConfig>,
  db: DbConfig,
}

#[derive(Serialize, Deserialize, Debug)]
struct DbConfig {
  username: String,
  password: String,
  database: String,
  hostname: String,
  hostport: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct PipelineConfig {
  name: String,
  check_interval: u64,
  http_client_timeout: u64,
  jenkins_base_url: String,
  jenkins_job_path: String,
  jenkins_wfapi_describe_path: String,
  jenkins_wfapi_runs_path: String,
  jenkins_api_describe_path: String,
  logical_split: String,
  http_auth_user: String,
  http_auth_pass: String,
  extra_config: Option<HashMap<String, String>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct WFAPIPipeline {
  name: String,
  #[serde(
    alias = "runCount",
    deserialize_with = "deserialize_number_from_string"
  )]
  run_count: i32,
  #[serde(alias = "_links")]
  links: HashMap<String, HashMap<String, String>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct WFAPIPipelineRunRef {
  #[serde(deserialize_with = "deserialize_number_from_string")]
  id: i32,
  name: String,
  #[serde(alias = "_links")]
  links: HashMap<String, HashMap<String, String>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct WFAPIPipelineRun {
  #[serde(deserialize_with = "deserialize_number_from_string")]
  id: i32,
  name: String,
  #[serde(alias = "startTimeMillis")]
  start_time_millis: u64,
  #[serde(alias = "endTimeMillis")]
  end_time_millis: u64,
  #[serde(alias = "durationMillis")]
  duration_millis: u64,
  #[serde(alias = "queueDurationMillis")]
  queue_duration_millis: u64,
  #[serde(alias = "pauseDurationMillis")]
  pause_duration_millis: u64,
  status: String,
  #[serde(alias = "stages")]
  stages_ref: Vec<WFAPIPipelineRunStageRef>,
  #[serde(skip)]
  stages: Vec<WFAPIPipelineRunStage>,
  #[serde(skip)]
  console_text: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct WFAPIPipelineRunStageRef {
  #[serde(deserialize_with = "deserialize_number_from_string")]
  id: i32,
  name: String,
  #[serde(alias = "_links")]
  links: HashMap<String, HashMap<String, String>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct WFAPIPipelineRunStage {
  #[serde(deserialize_with = "deserialize_number_from_string")]
  id: i32,
  name: String,
  #[serde(alias = "execNode")]
  exec_node: String,
  status: String,
  #[serde(alias = "startTimeMillis")]
  start_time_millis: u64,
  #[serde(alias = "durationMillis")]
  duration_millis: u64,
  #[serde(alias = "pauseDurationMillis")]
  pause_duration_millis: u64,
  #[serde(alias = "_links")]
  links: HashMap<String, HashMap<String, String>>,
  #[serde(alias = "stageFlowNodes")]
  stage_flow_nodes: Vec<WFAPIPipelineRunStageFlow>,
}

#[derive(Serialize, Deserialize, Debug)]
struct WFAPIPipelineRunStageFlow {
  #[serde(deserialize_with = "deserialize_number_from_string")]
  id: i32,
  name: String,
  #[serde(alias = "execNode")]
  exec_node: String,
  status: String,
  #[serde(alias = "parameterDescription")]
  parameter_description: String,
  #[serde(alias = "startTimeMillis")]
  start_time_millis: u64,
  #[serde(alias = "durationMillis")]
  duration_millis: u64,
  #[serde(alias = "pauseDurationMillis")]
  pause_duration_millis: u64,
  #[serde(alias = "_links")]
  links: HashMap<String, HashMap<String, String>>,
  #[serde(alias = "parentNodes")]
  parent_nodes: Vec<String>,
}

async fn get_wfapi_pipeline_runs(
  client: &reqwest::Client,
  pipeline: &PipelineConfig,
) -> std::result::Result<Vec<WFAPIPipelineRunRef>, reqwest::Error> {
  let request_url = format!(
    "{jenkins_base_url}{jenkins_describe_path}",
    jenkins_base_url = pipeline.jenkins_base_url,
    jenkins_describe_path = pipeline.jenkins_wfapi_runs_path
  );
  debug!("request: {}", request_url);
  let response = client.get(&request_url).send().await?;
  let runs: Vec<WFAPIPipelineRunRef> = response.json().await?;
  Ok(runs)
}

async fn get_wfapi_pipeline_run(
  client: &reqwest::Client,
  pipeline: &PipelineConfig,
  run_ref: &WFAPIPipelineRunRef,
) -> Result<WFAPIPipelineRun, reqwest::Error> {
  let request_url = format!(
    "{jenkins_base_url}{jenkins_describe_path}",
    jenkins_base_url = pipeline.jenkins_base_url,
    jenkins_describe_path = run_ref.links["self"]["href"]
  );
  debug!("request: {}", request_url);
  let response = client.get(&request_url).send().await?;
  let run: WFAPIPipelineRun = response.json().await?;
  Ok(run)
}

async fn get_pipeline_run_console_text(
  client: &reqwest::Client,
  pipeline: &PipelineConfig,
  run_ref: &WFAPIPipelineRunRef,
) -> Result<String, reqwest::Error> {
  let request_url = format!(
    "{jenkins_base_url}{jenkins_job_path}/{run_id}/consoleText",
    jenkins_base_url = pipeline.jenkins_base_url,
    jenkins_job_path = pipeline.jenkins_job_path,
    run_id = run_ref.id
  );
  debug!("request: {}", request_url);
  let response = client.get(&request_url).send().await?;
  let text: String = response.text().await?;
  Ok(text)
}

async fn get_wfapi_pipeline_run_stage(
  client: &reqwest::Client,
  pipeline: &PipelineConfig,
  stage_ref: &WFAPIPipelineRunStageRef,
) -> Result<WFAPIPipelineRunStage, reqwest::Error> {
  let request_url = format!(
    "{jenkins_base_url}{jenkins_describe_path}",
    jenkins_base_url = pipeline.jenkins_base_url,
    jenkins_describe_path = stage_ref.links["self"]["href"]
  );
  debug!("request: {}", request_url);
  let response = client.get(&request_url).send().await?;
  let stage: WFAPIPipelineRunStage = response.json().await?;
  Ok(stage)
}

async fn monitor_pipeline(
  pipeline: &PipelineConfig,
  pg_client: &Rc<Mutex<tokio_postgres::Client>>,
) -> Result<()> {
  let mut interval = time::interval(Duration::from_secs(pipeline.check_interval));
  let mut cnt = 0;

  let timeout = Duration::from_secs(pipeline.http_client_timeout);

  let mut b64buf = String::new();
  general_purpose::STANDARD.encode_string(
    format!("{}:{}", pipeline.http_auth_user, pipeline.http_auth_pass),
    &mut b64buf,
  );
  let auth_header_value = format!("Basic {}", b64buf);
  let mut auth_header = reqwest::header::HeaderValue::from_str(&auth_header_value)
    .expect("failed to set the auth header");
  auth_header.set_sensitive(true);
  let mut default_headers = reqwest::header::HeaderMap::new();
  default_headers.insert(reqwest::header::AUTHORIZATION, auth_header);
  let client = ClientBuilder::new()
    .connection_verbose(true)
    .connect_timeout(timeout)
    .timeout(timeout)
    .user_agent(APP_USER_AGENT)
    .default_headers(default_headers)
    .build()?;
  let pg_ps_pipeline_run = pg_insert_pipeline_run_ps(pg_client).await?;
  let pg_ps_pipeline_run_stage = pg_insert_pipeline_run_stage_ps(pg_client).await?;
  let pg_ps_pipeline_run_stage_flow = pg_insert_pipeline_run_stage_flow_ps(pg_client).await?;
  loop {
    info!("getting pipeline runs from {}[{}]", pipeline.name, cnt);
    let runs: Vec<WFAPIPipelineRunRef> = get_wfapi_pipeline_runs(&client, pipeline).await?;
    for r in runs {
      let mut run = get_wfapi_pipeline_run(&client, pipeline, &r).await?;
      run.console_text = get_pipeline_run_console_text(&client, pipeline, &r).await?;
      let db_insert_pipeline_run =
        pg_insert_pipeline_run(pg_client, &pg_ps_pipeline_run, pipeline, &run).await;
      match db_insert_pipeline_run {
        Ok(_) => info!("inserted successfuly"),
        Err(e) => error!("{}", e),
      }
      for s in &run.stages_ref {
        let pipeline_run_stage = get_wfapi_pipeline_run_stage(&client, pipeline, s).await?;
        debug!("{:?}", pipeline_run_stage);
        let db_insert_pipeline_run_stage = pg_insert_pipeline_run_stage(
          pg_client,
          &pg_ps_pipeline_run_stage,
          pipeline,
          &run,
          &pipeline_run_stage,
        )
        .await;
        match db_insert_pipeline_run_stage {
          Ok(_) => info!("inserted successfuly"),
          Err(e) => error!("{}", e),
        }
        for f in &pipeline_run_stage.stage_flow_nodes {
          let db_insert_pipeline_run_stage_flow = pg_insert_pipeline_run_stage_flow(
            pg_client,
            &pg_ps_pipeline_run_stage_flow,
            pipeline,
            &run,
            &pipeline_run_stage,
            f,
          )
          .await;
          match db_insert_pipeline_run_stage_flow {
            Ok(_) => info!("inserted successfuly"),
            Err(e) => error!("{}", e),
          }
        }
        run.stages.push(pipeline_run_stage);
      }
    }
    cnt += 1;
    interval.tick().await;
  }
}

async fn pg_destroy_db_structure(pg_client: &Rc<Mutex<tokio_postgres::Client>>) {
  let res = pg_client
    .lock()
    .await
    .batch_execute(&format!(
      "
            DROP SCHEMA IF EXISTS {db_schema} CASCADE;
            ",
      db_schema = DB_SCHEMA
    ))
    .await;

  match res {
    Ok(_) => warn!("pg_destroy_db_structure: database structure deleted with success"),
    Err(e) => error!("pg_destroy_db_structure: {}", e),
  }
}

async fn pg_create_db_structure(pg_client: &Rc<Mutex<tokio_postgres::Client>>) {
  let res = pg_client.lock().await.batch_execute(&format!("
            CREATE SCHEMA IF NOT EXISTS {db_schema};
            CREATE TABLE IF NOT EXISTS {db_schema}.pipeline_run (
                id            SERIAL PRIMARY KEY,
                pipeline_name VARCHAR(255),
                logical_split VARCHAR(255),
                run_id        INT4,
                status        VARCHAR(255),
                console_text  TEXT,
                created_at    TIMESTAMPTZ,
                updated_at    TIMESTAMPTZ
            );
            CREATE UNIQUE INDEX IF NOT EXISTS pipeline_run_unique ON {db_schema}.pipeline_run (pipeline_name, logical_split, run_id);
            CREATE TABLE IF NOT EXISTS {db_schema}.pipeline_run_stage (
                pipeline_run_id       INT4,
                pipeline_run_stage_id INT4,
                name                  VARCHAR(255),
                status                VARCHAR(255),
                exec_node             VARCHAR(255),
                created_at            TIMESTAMPTZ,
                updated_at            TIMESTAMPTZ
            );
            CREATE UNIQUE INDEX IF NOT EXISTS pipeline_run_stage_unique ON {db_schema}.pipeline_run_stage (pipeline_run_id, pipeline_run_stage_id);
            CREATE TABLE IF NOT EXISTS {db_schema}.pipeline_run_stage_flow (
                pipeline_run_id            INT4,
                pipeline_run_stage_id      INT4,
                pipeline_run_stage_flow_id INT4,
                name                  VARCHAR(255),
                status                VARCHAR(255),
                exec_node             VARCHAR(255),
                parameter_description VARCHAR(255),
                created_at            TIMESTAMPTZ,
                updated_at            TIMESTAMPTZ
            );
            CREATE UNIQUE INDEX IF NOT EXISTS pipeline_run_stage_flow_unique ON {db_schema}.pipeline_run_stage_flow (pipeline_run_id, pipeline_run_stage_id, pipeline_run_stage_flow_id);
            ", db_schema = DB_SCHEMA)).await;

  match res {
    Ok(_) => info!("pg_create_db_structure: database structure created with success"),
    Err(e) => error!("pg_create_db_structure: {}", e),
  }
}

async fn pg_insert_pipeline_run_ps(
  pg_client: &Rc<Mutex<tokio_postgres::Client>>,
) -> Result<tokio_postgres::Statement, tokio_postgres::Error> {
  let client = pg_client.lock().await;
  client
    .prepare_typed(
      &format!(
        "
            INSERT INTO {db_schema}.pipeline_run (
                pipeline_name, logical_split, run_id, status, console_text, created_at, updated_at
            ) VALUES (
            $1, $2, $3, $4, $5, NOW(), NOW()
            ) ON CONFLICT (pipeline_name, logical_split, run_id) DO UPDATE SET
            status = {db_schema}.pipeline_run.status,
            updated_at = NOW();
            ",
        db_schema = DB_SCHEMA
      ),
      &[
        Type::VARCHAR,
        Type::VARCHAR,
        Type::INT4,
        Type::VARCHAR,
        Type::TEXT,
      ],
    )
    .await
}

async fn pg_insert_pipeline_run(
  pg_client: &Rc<Mutex<tokio_postgres::Client>>,
  pg_statement: &tokio_postgres::Statement,
  pipeline: &PipelineConfig,
  run: &WFAPIPipelineRun,
) -> Result<(), tokio_postgres::Error> {
  let mut client = pg_client.lock().await;
  let transaction = client.build_transaction().start().await.unwrap(); // transaction().await;
  let res = transaction
    .execute(
      pg_statement,
      &[
        &pipeline.name,
        &pipeline.logical_split,
        &run.id,
        &run.status,
        &run.console_text,
      ],
    )
    .await;
  match res {
    Ok(_) => {
      info!("pg_insert_pipeline_run: pipeline={pipeline_name}, logical_split={logical_split}, run_id={run_id}", pipeline_name = pipeline.name, logical_split = pipeline.logical_split, run_id = run.id);
      transaction.commit().await
    }
    Err(e) => {
      error!("pg_insert_pipeline_run: {}", e);
      transaction.rollback().await
    }
  }
}

async fn pg_insert_pipeline_run_stage_ps(
  pg_client: &Rc<Mutex<tokio_postgres::Client>>,
) -> Result<tokio_postgres::Statement, tokio_postgres::Error> {
  let client = pg_client.lock().await;
  client.prepare_typed(&format!("
            INSERT INTO {db_schema}.pipeline_run_stage (
                pipeline_run_id, pipeline_run_stage_id, name, status, exec_node, created_at, updated_at
            ) VALUES (
            $1, $2, $3, $4, $5, NOW(), NOW()
            ) ON CONFLICT (pipeline_run_id, pipeline_run_stage_id) DO UPDATE SET
            status = {db_schema}.pipeline_run_stage.status,
            name = {db_schema}.pipeline_run_stage.name,
            exec_node = {db_schema}.pipeline_run_stage.exec_node,
            updated_at = NOW();
            ", db_schema = DB_SCHEMA), &[Type::INT4, Type::INT4, Type::VARCHAR, Type::VARCHAR, Type::VARCHAR]).await
}

async fn pg_insert_pipeline_run_stage(
  pg_client: &Rc<Mutex<tokio_postgres::Client>>,
  pg_statement: &tokio_postgres::Statement,
  pipeline: &PipelineConfig,
  run: &WFAPIPipelineRun,
  stage: &WFAPIPipelineRunStage,
) -> Result<(), tokio_postgres::Error> {
  let mut client = pg_client.lock().await;
  let transaction = client.build_transaction().start().await.unwrap();
  let res = transaction
    .execute(
      pg_statement,
      &[
        &run.id,
        &stage.id,
        &stage.name,
        &stage.status,
        &stage.exec_node,
      ],
    )
    .await;
  match res {
    Ok(_) => {
      info!(
        "pg_insert_pipeline_run_stage: pipeline={pipeline_name}",
        pipeline_name = pipeline.name
      );
      transaction.commit().await
    }
    Err(e) => {
      error!("{}", e);
      transaction.rollback().await
    }
  }
}

async fn pg_insert_pipeline_run_stage_flow_ps(
  pg_client: &Rc<Mutex<tokio_postgres::Client>>,
) -> Result<tokio_postgres::Statement, tokio_postgres::Error> {
  let client = pg_client.lock().await;
  client.prepare_typed(&format!("
            INSERT INTO {db_schema}.pipeline_run_stage_flow (
                pipeline_run_id, pipeline_run_stage_id, pipeline_run_stage_flow_id, name, status, exec_node, parameter_description, created_at, updated_at
            ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, NOW(), NOW()
            ) ON CONFLICT (pipeline_run_id, pipeline_run_stage_id, pipeline_run_stage_flow_id) DO UPDATE SET
            status = {db_schema}.pipeline_run_stage_flow.status,
            name = {db_schema}.pipeline_run_stage_flow.name,
            exec_node = {db_schema}.pipeline_run_stage_flow.exec_node,
            parameter_description = {db_schema}.pipeline_run_stage_flow.parameter_description,
            updated_at = NOW();
            ", db_schema = DB_SCHEMA), &[Type::INT4, Type::INT4, Type::INT4, Type::VARCHAR, Type::VARCHAR, Type::VARCHAR]).await
}

async fn pg_insert_pipeline_run_stage_flow(
  pg_client: &Rc<Mutex<tokio_postgres::Client>>,
  pg_statement: &tokio_postgres::Statement,
  pipeline: &PipelineConfig,
  run: &WFAPIPipelineRun,
  stage: &WFAPIPipelineRunStage,
  flow: &WFAPIPipelineRunStageFlow,
) -> Result<(), tokio_postgres::Error> {
  let mut client = pg_client.lock().await;
  let transaction = client.build_transaction().start().await.unwrap();
  let res = transaction
    .execute(
      pg_statement,
      &[
        &run.id,
        &stage.id,
        &flow.id,
        &flow.name,
        &flow.status,
        &flow.exec_node,
        &flow.parameter_description,
      ],
    )
    .await;
  match res {
    Ok(_) => {
      info!(
        "pg_insert_pipeline_run_stage_flow: pipeline={pipeline_name}",
        pipeline_name = pipeline.name
      );
      transaction.commit().await
    }
    Err(e) => {
      error!("pg_insert_pipeline_run_stage_flow: {}", e);
      transaction.rollback().await
    }
  }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  env_logger::init();
  dotenvy::dotenv().expect("could not open the .env file");
  let config_file = std::fs::File::open("config.yml").expect("could not open the config.yml file");
  let config: Config =
    serde_yaml::from_reader(config_file).expect("could not read the values of the config file");
  let (raw_pg_client, pg_connection) = tokio_postgres::connect(
    &format!(
      "host={hostname} port={hostport} user={username} password={password}",
      hostname = config.db.hostname,
      hostport = config.db.hostport,
      username = config.db.username,
      password = config.db.password
    ),
    NoTls,
  )
  .await?;

  let pg_client = Rc::new(Mutex::new(raw_pg_client));

  tokio::spawn(async move {
    pg_connection.await.expect("pg connection error");
  });

  if env::var_os("SPLINTER_DROP_ON_STARTUP").unwrap_or("0".into()) == "1" {
    warn!("dropping the database on the startup");
    pg_destroy_db_structure(&pg_client).await;
  }

  pg_create_db_structure(&pg_client).await;

  // This is a simplistic multiple async loops with with time wait logic.
  // For now this implementation will sulfice but I have some ideas
  // to improve this mechanism using streams & futures
  let tasks = config.pipelines.iter().map(|p| async {
    let _ = monitor_pipeline(p, &pg_client).await;
  });

  join_all(tasks).await;
  Ok(())
}
