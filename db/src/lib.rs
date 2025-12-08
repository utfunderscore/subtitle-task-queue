use crate::models::{NewSegment, NewTask, SegmentRow, Task};
use crate::schema::segment::dsl::segment;
use crate::schema::task::dsl::task;
use crate::schema::task::stage;
use anyhow::{Context, Result};
use chrono::Utc;
use common::{Segment, TaskStage};
use diesel::QueryDsl;
use diesel::associations::HasTable;
use diesel::r2d2::ConnectionManager;
use diesel::{ExpressionMethods, PgConnection, RunQueryDsl, SelectableHelper};
use r2d2::Pool;
use tokio::task::spawn_blocking;

pub mod models;
pub mod schema;

#[derive(Clone)]
pub struct TaskStore {
    pg_connection: Pool<ConnectionManager<PgConnection>>,
}

impl TaskStore {
    pub fn new(database_url: &str) -> Result<Self> {
        let connection = connect(database_url)?;
        Ok(Self {
            pg_connection: connection,
        })
    }

    pub async fn new_task(&mut self, task_stage: TaskStage, context: &'static str) -> Result<i32> {
        let pool = self.pg_connection.clone();
        spawn_blocking(move || {
            let mut conn = pool
                .get()
                .context("Failed to acquire database connection from pool")?;

            diesel::insert_into(task::table())
                .values(NewTask::new(
                    Utc::now(),
                    task_stage.to_string(),
                    String::from(context),
                ))
                .returning(Task::as_returning())
                .get_result(&mut conn)
                .map(|t: Task| t.id)
                .context("Failed to insert new task into database")
        })
        .await
        .context("Async task execution failed")?
    }

    pub async fn set_task_stage(
        &mut self,
        task_id: i32,
        task_stage: TaskStage,
        context: String,
    ) -> Result<()> {
        let pool = self.pg_connection.clone();
        spawn_blocking(move || {
            println!("Updating task: {} {} {}", task_id, task_stage, context);

            let mut conn = pool
                .get()
                .context("Failed to acquire database connection from pool")?;

            diesel::update(task.filter(schema::task::id.eq(task_id)))
                .set((
                    stage.eq(task_stage.to_string()),
                    schema::task::context.eq(context),
                ))
                .execute(&mut conn)?;

            Ok(())
        })
        .await
        .context("Async task execution failed")?
    }

    pub async fn store_segments(&mut self, task_id: i32, segments: Vec<Segment>) -> Result<()> {
        let rows: Vec<NewSegment> = segments
            .into_iter()
            .map(|seg: Segment| NewSegment::new(task_id, seg))
            .collect();

        let pool = self.pg_connection.clone();
        spawn_blocking(move || {
            let mut conn = pool
                .get()
                .context("Failed to acquire database connection from pool")?;

            diesel::insert_into(segment::table())
                .values(rows)
                .execute(&mut conn)
                .context("Failed to insert segments into database")
        })
        .await
        .context("Async task execution failed")??;

        Ok(())
    }

    pub async fn get_segments(&mut self, task_id: i32) -> Result<Vec<Segment>> {
        let pool = self.pg_connection.clone();

        let results = spawn_blocking(move || {
            let mut conn = pool
                .get()
                .context("Failed to acquire database connection from pool")?;

            schema::segment::table
                .filter(schema::segment::task_id.eq(task_id))
                .load::<SegmentRow>(&mut conn)
                .context("Failed to get segments")
        })
        .await
        .context("Async task execution failed")??;

        Ok(results
            .into_iter()
            .map(|x| Segment::new(x.text, x.start_timestamp, x.end_timestamp))
            .collect())
    }
}

pub fn connect(database_url: &str) -> Result<Pool<ConnectionManager<PgConnection>>> {
    let manager = ConnectionManager::<PgConnection>::new(database_url);
    Pool::builder()
        .build(manager)
        .context("Failed to create database connection pool")
}
