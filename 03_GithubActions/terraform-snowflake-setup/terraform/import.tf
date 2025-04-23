# アカウントロールを作成
import{
  for_each = { for data in local.roles : data.name => data }

  id = each.value.name
  to = snowflake_account_role.roles[each.key]
}

# アカウントオブジェクト権限をアカウントロールに付与
import{
  for_each = { for data in local.priv_accobjs : "${data.role}-${data.object_name}" => data }

  id = "${each.value.role}|false|false|${join(",",each.value.privileges)}|OnAccountObject|${each.value.object_type}|${each.value.object_name}"
  to = snowflake_grant_privileges_to_account_role.priv_accobjs[each.key]
}

# スキーマ権限をアカウントロールに付与
import{
  for_each = { for data in local.priv_schems : "${data.role}-${data.schema}" => data }

  id = "${each.value.role}|false|false|${join(",",each.value.privileges)}|OnSchema|OnSchema|${each.value.schema}"
  to = snowflake_grant_privileges_to_account_role.priv_schems[each.key]
}

# スキーマ配下オブジェクト権限(ALL)をアカウントロールに付与
import{
  for_each = { for data in local.priv_schobj_alls : "${data.role}-${data.schema}-${data.object_type}-all" => data }

  id = "${each.value.role}|false|false|${join(",",each.value.privileges)}|OnSchemaObject|OnAll|${each.value.object_type}|InSchema|${each.value.schema}"
  to = snowflake_grant_privileges_to_account_role.priv_schobj_alls[each.key]
}

# スキーマ配下オブジェクト権限(FUTURE)をアカウントロールに付与
import{
  for_each = { for data in local.priv_schobj_futrs : "${data.role}-${data.schema}-${data.object_type}-futr" => data }

  id = "${each.value.role}|false|false|${join(",",each.value.privileges)}|OnSchemaObject|OnFuture|${each.value.object_type}|InSchema|${each.value.schema}"
  to = snowflake_grant_privileges_to_account_role.priv_schobj_futrs[each.key]
}

# スキーマ配下オブジェクト権限(OWNERSHIP/ALL)をアカウントロールに付与
import{
  for_each = { for data in local.priv_schobj_own_alls : "${data.role}-${data.schema}-${data.object_type}-own-all" => data }

  id = "ToAccountRole|${each.value.role}|COPY|OnAll|${each.value.object_type}|InSchema|${each.value.schema}"
  to = snowflake_grant_ownership.priv_schobj_own_alls[each.key]
}

# スキーマ配下オブジェクト権限(OWNERSHIP/FUTURE)をアカウントロールに付与
import{
  for_each = { for data in local.priv_schobj_own_futrs : "${data.role}-${data.schema}-${data.object_type}-own-futr" => data }

  id = "ToAccountRole|${each.value.role}|COPY|OnFuture|${each.value.object_type}|InSchema|${each.value.schema}"
  to = snowflake_grant_ownership.priv_schobj_own_futrs[each.key]
}

# スキーマ配下オブジェクト権限(OWNERSHIP)をアカウントロールに付与
import{
  for_each = { for data in local.priv_schobj_own : "${data.role}-${data.object_name}-own" => data }

  id = "ToAccountRole|${each.value.role}|COPY|OnObject|${each.value.object_type}|${each.value.object_name}"
  to = snowflake_grant_ownership.priv_schobj_own[each.key]
}

# スキーマ配下オブジェクト権限をアカウントロールに付与
import{
  for_each = { for data in local.priv_schobjs : "${data.role}-${data.object_name}" => data }

  id = "${each.value.role}|false|false|${join(",",each.value.privileges)}|OnSchemaObject|OnObject|${each.value.object_type}|${each.value.object_name}"
  to = snowflake_grant_privileges_to_account_role.priv_schobjs[each.key]
}

# グローバル権限をアカウントロールに付与
import{
  for_each = { for data in local.priv_globals : "${data.role}-global" => data }

  id = "${each.value.role}|false|false|${join(",",each.value.privileges)}|OnAccount"
  to = snowflake_grant_privileges_to_account_role.priv_globals[each.key]
}

# アカウントロールにアカウントロールの権限を付与(親ロール)
import{
  for_each = { for data in local.priv_roles : "${data.role}-${data.parent_role}" => data }

  id = "\"${each.value.role}\"|ROLE|\"${each.value.parent_role}\""
  to = snowflake_grant_account_role.priv_roles[each.key]
}

# アカウントロールにデータベースロールの権限を付与(親ロール)
import{
  for_each = { for data in local.priv_db_roles : "${data.database}-${data.db_role}-${data.parent_role}" => data }

  id = "\"${each.value.database}\".\"${each.value.db_role}\"|ROLE|\"${each.value.parent_role}\""
  to = snowflake_grant_database_role.priv_db_roles[each.key]
}

# データベースロールを作成
import{
  for_each = { for data in local.db_roles : data.name => data }

  id = "${each.value.database}.${each.value.name}"
  to = snowflake_database_role.db_roles[each.key]
}

# データベース権限をデータベースロールに付与
import{
  for_each = { for data in local.dr_priv_accobjs : "${data.db_role}-${data.database}" => data }

  id = "${each.value.db_role}|false|false|${join(",",each.value.privileges)}|OnDatabase|${each.value.database}"
  to = snowflake_grant_privileges_to_database_role.dr_priv_accobjs[each.key]
}

# スキーマ権限をデータベースロールに付与
import{
  for_each = { for data in local.dr_priv_schems : "${data.db_role}-${data.schema}" => data }

  id = "${each.value.db_role}|false|false|${join(",",each.value.privileges)}|OnSchema|OnSchema|${each.value.schema}"
  to = snowflake_grant_privileges_to_database_role.dr_priv_schems[each.key]
}

# スキーマ配下オブジェクト権限(ALL)をデータベースロールに付与
import{
  for_each = { for data in local.dr_priv_schobj_alls : "${data.db_role}-${data.schema}-${data.object_type}-all" => data }

  id = "${each.value.db_role}|false|false|${join(",",each.value.privileges)}|OnSchemaObject|OnAll|${each.value.object_type}|InSchema|${each.value.schema}"
  to = snowflake_grant_privileges_to_database_role.dr_priv_schobj_alls[each.key]
}

# スキーマ配下オブジェクト権限(FUTURE)をデータベースロールに付与
import{
  for_each = { for data in local.dr_priv_schobj_futrs : "${data.db_role}-${data.schema}-${data.object_type}-futr" => data }

  id = "${each.value.db_role}|false|false|${join(",",each.value.privileges)}|OnSchemaObject|OnFuture|${each.value.object_type}|InSchema|${each.value.schema}"
  to = snowflake_grant_privileges_to_database_role.dr_priv_schobj_futrs[each.key]
}

# スキーマ配下オブジェクト権限をデータベースロールに付与
import{
  for_each = { for data in local.dr_priv_schobjs : "${data.db_role}-${data.object_name}" => data }

  id = "${each.value.db_role}|false|false|${join(",",each.value.privileges)}|OnSchemaObject|OnObject|${each.value.object_type}|${each.value.object_name}"
  to = snowflake_grant_privileges_to_database_role.dr_priv_schobjs[each.key]
}

# ユーザを作成
import{
  for_each = { for data in local.users : data.name => data }

  id = each.value.name
  to = snowflake_user.users[each.key]
}

# ユーザにアカウントロールの権限を付与(親ロール)
import{
  for_each = { for data in local.priv_role_users : "${data.user}-${data.role}" => data }

  id = "\"${each.value.role}\"|USER|\"${each.value.user}\""
  to = snowflake_grant_account_role.priv_role_users[each.key]
}
