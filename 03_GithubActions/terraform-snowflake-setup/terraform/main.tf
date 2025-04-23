# アカウントロールを作成
resource "snowflake_account_role" "roles" {
  for_each = { for data in local.roles : data.name => data }

  name    = each.value.name
  comment = lookup(each.value, "comment", null)
}

# アカウントオブジェクト権限をアカウントロールに付与
resource "snowflake_grant_privileges_to_account_role" "priv_accobjs" {
  for_each = { for data in local.priv_accobjs : "${data.role}-${data.object_name}" => data }

  privileges        = lookup(each.value, "privileges", null)
  account_role_name = each.value.role
  on_account_object {
    object_type     = lookup(each.value, "object_type", null)
    object_name     = lookup(each.value, "object_name", null)
  }

  depends_on = [snowflake_account_role.roles]
}

# スキーマ権限をアカウントロールに付与
resource "snowflake_grant_privileges_to_account_role" "priv_schems" {
  for_each = { for data in local.priv_schems : "${data.role}-${data.schema}" => data }

  privileges        = lookup(each.value, "privileges", null)
  account_role_name = each.value.role
  on_schema {
    schema_name     = lookup(each.value, "schema", null)
  }

  depends_on = [snowflake_account_role.roles]
}

# スキーマ配下オブジェクト権限(ALL)をアカウントロールに付与
resource "snowflake_grant_privileges_to_account_role" "priv_schobj_alls" {
  for_each = { for data in local.priv_schobj_alls : "${data.role}-${data.schema}-${data.object_type}-all" => data }

  privileges             = lookup(each.value, "privileges", null)
  account_role_name      = each.value.role
  on_schema_object {
    all {
      object_type_plural = lookup(each.value, "object_type", null)
      in_schema          = lookup(each.value, "schema", null)
    }
  }

  depends_on = [snowflake_account_role.roles]
}

# スキーマ配下オブジェクト権限(FUTURE)をアカウントロールに付与
resource "snowflake_grant_privileges_to_account_role" "priv_schobj_futrs" {
  for_each = { for data in local.priv_schobj_futrs : "${data.role}-${data.schema}-${data.object_type}-futr" => data }

  privileges             = lookup(each.value, "privileges", null)
  account_role_name      = each.value.role
  on_schema_object {
    future {
      object_type_plural = lookup(each.value, "object_type", null)
      in_schema          = lookup(each.value, "schema", null)
    }
  }

  depends_on = [snowflake_account_role.roles]
}

# スキーマ配下オブジェクト権限をアカウントロールに付与
resource "snowflake_grant_privileges_to_account_role" "priv_schobjs" {
  for_each = { for data in local.priv_schobjs : "${data.role}-${data.object_name}" => data }

  privileges        = lookup(each.value, "privileges", null)
  account_role_name = each.value.role
  on_schema_object {
    object_type     = lookup(each.value, "object_type", null)
    object_name     = lookup(each.value, "object_name", null)
  }

  depends_on = [snowflake_account_role.roles]
}

# グローバル権限をアカウントロールに付与
resource "snowflake_grant_privileges_to_account_role" "priv_globals" {
  for_each = { for data in local.priv_globals : "${data.role}-global" => data }

  privileges        = lookup(each.value, "privileges", null)
  account_role_name = each.value.role
  on_account        = true

  depends_on = [snowflake_account_role.roles]
}

# スキーマ配下オブジェクト権限(OWNERSHIP/ALL)をアカウントロールに付与
resource "snowflake_grant_ownership" "priv_schobj_own_alls" {
  for_each = { for data in local.priv_schobj_own_alls : "${data.role}-${data.schema}-${data.object_type}-own-all" => data }

  account_role_name      = each.value.role
  outbound_privileges = "COPY"
  on {
    all {
      object_type_plural = lookup(each.value, "object_type", null)
      in_schema          = lookup(each.value, "schema", null)
    }
  }

  depends_on = [snowflake_account_role.roles]
}

# スキーマ配下オブジェクト権限(OWNERSHIP/FUTURE)をアカウントロールに付与
resource "snowflake_grant_ownership" "priv_schobj_own_futrs" {
  for_each = { for data in local.priv_schobj_own_futrs : "${data.role}-${data.schema}-${data.object_type}-own-futr" => data }

  account_role_name      = each.value.role
  outbound_privileges = "COPY"
  on {
    future {
      object_type_plural = lookup(each.value, "object_type", null)
      in_schema          = lookup(each.value, "schema", null)
    }
  }
  depends_on = [snowflake_account_role.roles]
}

# スキーマ配下オブジェクト権限(OWNERSHIP)をアカウントロールに付与
resource "snowflake_grant_ownership" "priv_schobj_own" {
  for_each = { for data in local.priv_schobj_own : "${data.role}-${data.object_name}-own" => data }

  account_role_name   = each.value.role
  outbound_privileges = "COPY"
  on {
    object_type       = lookup(each.value, "object_type", null)
    object_name       = lookup(each.value, "object_name", null)
  }
  depends_on = [snowflake_account_role.roles]
}

# アカウントロールにアカウントロールの権限を付与(親ロール)
resource "snowflake_grant_account_role" "priv_roles" {
  for_each = { for data in local.priv_roles : "${data.role}-${data.parent_role}" => data }

  role_name        = each.value.role
  parent_role_name = lookup(each.value, "parent_role", null)

  depends_on = [snowflake_account_role.roles]
}

# アカウントロールにデータベースロールの権限を付与(親ロール)
resource "snowflake_grant_database_role" "priv_db_roles" {
  for_each = { for data in local.priv_db_roles : "${data.database}-${data.db_role}-${data.parent_role}" => data }

  database_role_name = format("%s.%s", each.value.database, each.value.db_role)
  parent_role_name   = lookup(each.value, "parent_role", null)

  depends_on = [snowflake_database_role.db_roles]
}

# データベースロールを作成
resource "snowflake_database_role" "db_roles" {
  provider = snowflake.sysadmin
  for_each = { for data in local.db_roles : data.name => data }

  name     = each.value.name
  database = lookup(each.value, "database", null)
  comment  = lookup(each.value, "comment", null)

  depends_on = [snowflake_account_role.roles]
}

# データベース権限をデータベースロールに付与
resource "snowflake_grant_privileges_to_database_role" "dr_priv_accobjs" {
  for_each = { for data in local.dr_priv_accobjs : "${data.db_role}-${data.database}" => data }

  privileges         = lookup(each.value, "privileges", null)
  database_role_name = each.value.db_role
  on_database        = lookup(each.value, "database", null)

  depends_on = [snowflake_database_role.db_roles]
}

# スキーマ権限をデータベースロールに付与
resource "snowflake_grant_privileges_to_database_role" "dr_priv_schems" {
  for_each = { for data in local.dr_priv_schems : "${data.db_role}-${data.schema}" => data }

  privileges         = lookup(each.value, "privileges", null)
  database_role_name = each.value.db_role
  on_schema {
    schema_name      = lookup(each.value, "schema", null)
  }

  depends_on = [snowflake_database_role.db_roles]
}

# スキーマ配下オブジェクト権限(ALL)をデータベースロールに付与
resource "snowflake_grant_privileges_to_database_role" "dr_priv_schobj_alls" {
  for_each = { for data in local.dr_priv_schobj_alls : "${data.db_role}-${data.schema}-${data.object_type}-all" => data }

  privileges             = lookup(each.value, "privileges", null)
  database_role_name     = each.value.db_role
  on_schema_object {
    all {
      object_type_plural = lookup(each.value, "object_type", null)
      in_schema          = lookup(each.value, "schema", null)
    }
  }

  depends_on = [snowflake_database_role.db_roles]
}

# スキーマ配下オブジェクト権限(FUTURE)をデータベースロールに付与
resource "snowflake_grant_privileges_to_database_role" "dr_priv_schobj_futrs" {
  for_each = { for data in local.dr_priv_schobj_futrs : "${data.db_role}-${data.schema}-${data.object_type}-futr" => data }

  privileges             = lookup(each.value, "privileges", null)
  database_role_name     = each.value.db_role
  on_schema_object {
    future {
      object_type_plural = lookup(each.value, "object_type", null)
      in_schema          = lookup(each.value, "schema", null)
    }
  }

  depends_on = [snowflake_database_role.db_roles]
}

# スキーマ配下オブジェクト権限をデータベースロールに付与
resource "snowflake_grant_privileges_to_database_role" "dr_priv_schobjs" {
  for_each = { for data in local.dr_priv_schobjs : "${data.db_role}-${data.object_name}" => data }

  privileges         = lookup(each.value, "privileges", null)
  database_role_name = each.value.db_role
  on_schema_object {
    object_type      = lookup(each.value, "object_type", null)
    object_name      = lookup(each.value, "object_name", null)
  }

  depends_on = [snowflake_database_role.db_roles]
}

# ユーザを作成
resource "snowflake_user" "users" {
  for_each = { for data in local.users : data.name => data }

  name                           = each.value.name
  comment                        = lookup(each.value, "comment", null)
  default_warehouse              = lookup(each.value, "default_warehouse", null)
  default_secondary_roles_option = lookup(each.value, "default_secondary_roles_option", null)
  default_role                   = lookup(each.value, "default_role", null)
  default_namespace              = lookup(each.value, "default_namespace", null)
  rsa_public_key                 = lookup(each.value, "rsa_public_key", null)
  rsa_public_key_2               = lookup(each.value, "rsa_public_key_2", null)
  client_session_keep_alive      = lookup(each.value, "client_session_keep_alive", null)
  network_policy                 = lookup(each.value, "network_policy", null)
  statement_timeout_in_seconds   = lookup(each.value, "statement_timeout_in_seconds", null)

  display_name                   = each.value.name

  depends_on = [snowflake_database_role.db_roles]
}

# ユーザにアカウントロールの権限を付与(親ロール)
resource "snowflake_grant_account_role" "priv_role_users" {
  for_each = { for data in local.priv_role_users : "${data.user}-${data.role}" => data }

  role_name = each.value.role
  user_name = lookup(each.value, "user", null)

  depends_on = [snowflake_user.users]
}
