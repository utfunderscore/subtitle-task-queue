// @generated automatically by Diesel CLI.

diesel::table! {
    segment (id) {
        id -> Int4,
        task_id -> Int4,
        text -> Text,
        start_timestamp -> Int8,
        end_timestamp -> Int8,
    }
}

diesel::table! {
    task (id) {
        id -> Int4,
        created_at -> Timestamptz,
        #[max_length = 20]
        stage -> Varchar,
        context -> Text,
    }
}

diesel::joinable!(segment -> task (task_id));

diesel::allow_tables_to_appear_in_same_query!(segment, task,);
