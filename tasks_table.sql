CREATE SEQUENCE tasks_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;

CREATE TABLE tasks (
	id integer NOT NULL DEFAULT nextval('tasks_id_seq'::regclass),
	user_id integer, -- The creator of the task
	start_date TIMESTAMP(0) WITHOUT TIME ZONE DEFAULT (NOW()) NOT NULL,
	end_date TIMESTAMP(0) WITHOUT TIME ZONE,
	status varchar, -- cancelled, error, in progress, completed, delegated
	progress_percent integer,
	type varchar, -- The subject of the task. Used in the Python scripts to filter tasks
	is_visible BOOLEAN NOT NULL DEFAULT FALSE, -- Some tasks are internals
	related_task_id integer REFERENCES tasks (id), -- If the task has been created by another task, the parent task id is here
	request jsonb,
	result jsonb,    
	CONSTRAINT pk_tasks_id PRIMARY KEY (id)
);
