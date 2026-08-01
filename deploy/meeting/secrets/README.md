# Runtime secret files

Create these four files locally. They are ignored by Git and mounted read-only:

- `discord_bot_token`: token of the official test bot application.
- `meeting_integration_token`: bearer shared with the Meeting Platform ingestion service.
- `postgres_password`: password used by the isolated PostgreSQL instance.
- `database_url`: `postgresql://craig_meeting:<same-password>@db:5432/craig_meeting?schema=public`.

Do not use a Discord user token, a public guild, or production credentials.
