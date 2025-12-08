use std::str::FromStr;
use common::Segment;
use diesel::prelude::*;

#[derive(Queryable, Selectable, Identifiable, Debug)]
#[diesel(table_name = crate::schema::task)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Task {
    pub id: i32,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::task)]
pub struct NewTask {
    created_at: chrono::DateTime<chrono::Utc>,
    stage: String,
    context: String,
}

impl NewTask {
    pub fn new(created_at: chrono::DateTime<chrono::Utc>, stage: String, context: String) -> Self {
        Self { created_at, stage, context }
    }
}

#[derive(Queryable, Selectable, Identifiable, Associations, Debug)]
#[diesel(belongs_to(Task))]
#[diesel(table_name = crate::schema::segment)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct SegmentRow {
    pub id: i32,
    pub task_id: i32,
    pub text: String,
    pub start_timestamp: i64,
    pub end_timestamp: i64,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = crate::schema::segment)]
pub struct NewSegment {
    pub task_id: i32,
    pub text: String,
    pub start_timestamp: i64,
    pub end_timestamp: i64,
}

impl NewSegment {
    pub fn new(task_id: i32, segment: Segment) -> Self {
        Self {
            task_id,
            text: segment.text,
            start_timestamp: segment.start_timestamp,
            end_timestamp: segment.end_timestamp,
        }
    }
}
