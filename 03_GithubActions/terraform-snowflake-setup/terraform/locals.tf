locals {
  role_files = fileset("${path.module}/../yml/role", "*.yml")

  db_role_files = fileset("${path.module}/../yml/database_role", "*.yml")

  user_files = fileset("${path.module}/../yml/user", "*.yml")

  role_data = [
    for file in local.role_files : try(
      yamldecode(file("${path.module}/../yml/role/${file}")),
      {}
    )
  ]

  db_role_data = [
    for file in local.db_role_files : try(
      yamldecode(file("${path.module}/../yml/database_role/${file}")),
      {}
    )
  ]

  user_data = [
    for file in local.user_files : try(
      yamldecode(file("${path.module}/../yml/user/${file}")),
      {}
    )
  ]

  roles = flatten([
    for data in local.role_data : data.role if can(data.role)
  ])

  priv_accobjs = flatten([
    for data in local.role_data : data.priv_accobj if can(data.priv_accobj)
  ])

  priv_schems = flatten([
    for data in local.role_data : data.priv_schem if can(data.priv_schem)
  ])
  
  priv_schobj_alls = flatten([
    for data in local.role_data : data.priv_schobj_all if can(data.priv_schobj_all)
  ])

  priv_schobj_futrs = flatten([
    for data in local.role_data : data.priv_schobj_futr if can(data.priv_schobj_futr)
  ])

  priv_schobj_own_alls = flatten([
    for data in local.role_data : data.priv_schobj_own_all if can(data.priv_schobj_own_all)
  ])

  priv_schobj_own_futrs = flatten([
    for data in local.role_data : data.priv_schobj_own_futr if can(data.priv_schobj_own_futr)
  ])

  priv_schobj_own = flatten([
    for data in local.role_data : data.priv_schobj_own if can(data.priv_schobj_own)
  ])

  priv_schobjs = flatten([
    for data in local.role_data : data.priv_schobj if can(data.priv_schobj)
  ])

  priv_globals = flatten([
    for data in local.role_data : data.priv_global if can(data.priv_global)
  ])

  priv_roles = flatten([
    for data in local.role_data : data.priv_role if can(data.priv_role)
  ])

  db_roles = flatten([
    for data in local.db_role_data : data.db_role if can(data.db_role)
  ])

  dr_priv_accobjs = flatten([
    for data in local.db_role_data : data.dr_priv_accobj if can(data.dr_priv_accobj)
  ])

  dr_priv_schems = flatten([
    for data in local.db_role_data : data.dr_priv_schem if can(data.dr_priv_schem)
  ])
  
  dr_priv_schobj_alls = flatten([
    for data in local.db_role_data : data.dr_priv_schobj_all if can(data.dr_priv_schobj_all)
  ])

  dr_priv_schobj_futrs = flatten([
    for data in local.db_role_data : data.dr_priv_schobj_futr if can(data.dr_priv_schobj_futr)
  ])

  dr_priv_schobjs = flatten([
    for data in local.db_role_data : data.dr_priv_schobj if can(data.dr_priv_schobj)
  ])

  priv_db_roles = flatten([
    for data in local.db_role_data : data.priv_db_role if can(data.priv_db_role)
  ])

  users = flatten([
    for data in local.user_data : data.user if can(data.user)
  ])

  priv_role_users = flatten([
    for data in local.user_data : data.priv_role_user if can(data.priv_role_user)
  ])
}
