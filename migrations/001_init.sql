-- Align local schema with Supabase leadgen schema
create schema if not exists leadgen;

create extension if not exists "pgcrypto";

-- Enum used for relation roles
do $$
begin
  if not exists (select 1 from pg_type typ join pg_namespace nsp on nsp.oid = typ.typnamespace where typ.typname = 'person_role' and nsp.nspname = 'leadgen') then
    create type leadgen.person_role as enum ('contact_person', 'decision_maker', 'recruiter', 'other');
  end if;
end$$;

create table if not exists leadgen.companies (
  id uuid not null default gen_random_uuid(),
  company_key text not null,
  name text null,
  domain text null,
  orgnr text null,
  proff_url text null,
  industry text null,
  company_size text null,
  location text null,
  created_at timestamptz null default now(),
  updated_at timestamptz null default now(),
  profit_before_tax text null,
  turnover text null,
  clean_domain text null,
  constraint companies_pkey primary key (id),
  constraint companies_company_key_key unique (company_key)
);

create table if not exists leadgen.people (
  id uuid not null default gen_random_uuid(),
  person_key text null,
  full_name text not null,
  title text null,
  email text null,
  phone text null,
  linkedin_url text null,
  created_at timestamptz null default now(),
  updated_at timestamptz null default now(),
  normalized_company_name text null,
  normalized_company_domain text null,
  constraint people_pkey primary key (id),
  constraint people_person_key_key unique (person_key)
);

create table if not exists leadgen.job_posts (
  id uuid not null default gen_random_uuid(),
  finn_url text not null,
  company_id uuid not null,
  company_name_raw text null,
  job_title text null,
  description text null,
  application_url text null,
  location text null,
  employment_type text null,
  publication_date timestamptz null,
  expiration_date_raw text null,
  company_logo_url text null,
  scraped_at timestamptz null,
  status text null default 'new',
  created_at timestamptz null default now(),
  constraint job_posts_pkey primary key (id),
  constraint job_posts_finn_url_key unique (finn_url),
  constraint job_posts_company_id_fkey foreign key (company_id) references leadgen.companies (id) on delete cascade
);

create table if not exists leadgen.company_people (
  company_id uuid not null,
  person_id uuid not null,
  role leadgen.person_role not null,
  created_at timestamptz null default now(),
  constraint company_people_pkey primary key (company_id, person_id, role),
  constraint company_people_company_id_fkey foreign key (company_id) references leadgen.companies (id) on delete cascade,
  constraint company_people_person_id_fkey foreign key (person_id) references leadgen.people (id) on delete cascade
);

create table if not exists leadgen.job_post_people (
  job_post_id uuid not null,
  person_id uuid not null,
  role leadgen.person_role not null,
  source text null,
  created_at timestamptz null default now(),
  constraint job_post_people_pkey primary key (job_post_id, person_id, role),
  constraint job_post_people_job_post_id_fkey foreign key (job_post_id) references leadgen.job_posts (id) on delete cascade,
  constraint job_post_people_person_id_fkey foreign key (person_id) references leadgen.people (id) on delete cascade
);

create index if not exists people_email_idx on leadgen.people using btree (email);
create index if not exists people_linkedin_idx on leadgen.people using btree (linkedin_url);
create index if not exists job_posts_company_id_idx on leadgen.job_posts using btree (company_id);
