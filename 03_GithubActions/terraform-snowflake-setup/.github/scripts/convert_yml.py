import os
import glob
import yaml

# ロールyml生成
def transform_role_yml(file_path):
    data = load_yml(file_path)

    # system_roleキーが存在する場合、それを基にroleキーを生成
    system_name = None
    if 'system_role' in data:
        system_role_data = data.pop('system_role')
        system_name = system_role_data.get('system_name')
        if system_name is None:
            print(f"【Warning】{file_path}のsystem_roleキーにsystem_nameキーが存在しません")
            return
        system_role = {
            "name": f"{system_name}_ROLE",
            **{k: v for k, v in system_role_data.items() if k != 'system_name'}
        }
        data['role'] = system_role

    # bind_roleキーが存在する場合、それを基にroleキーを生成
    database_name = None
    schema_name = None
    if 'bind_role' in data:
        bind_role_data = data.pop('bind_role')
        database_name = bind_role_data.get('database_name')
        schema_name = bind_role_data.get('schema_name')
        if database_name is None or schema_name is None:
            print(f"【Warning】{file_path}のbind_roleキーにdatabase_name/schema_nameキーが存在しません")
            return
        bind_role = {
            "name": f"BIND_{database_name}_{schema_name}_READ_ROLE",
            **{k: v for k, v in bind_role_data.items() if k not in ['database_name', 'schema_name']}
        }
        data['role'] = bind_role

    # bind_user/publicationdb_write/read_roleキーが存在する場合、それを基にroleキーを生成
    user_project_id = None
    bind_role = None
    bind_roles = ['bind_userdb_read_role', 'bind_userdb_write_role', 'bind_publicationdb_read_role', 'bind_publicationdb_write_role']
    if (bind_role := next((r for r in bind_roles if r in data), None)):
        bind_role_data = data.get(bind_role)
        user_project_id = bind_role_data.get('user_project_id')
        if user_project_id is None:
            print(f"【Warning】{file_path}のbind_roleキーにuser_project_idキーが存在しません")
            return
        if bind_role == 'bind_userdb_read_role':
            bind_name = f"BIND_USER_{user_project_id}_READ_ROLE"
        elif bind_role == 'bind_userdb_write_role':
            bind_name = f"BIND_USER_{user_project_id}_WRITE_ROLE"
        elif bind_role == 'bind_publicationdb_read_role':
            bind_name = f"BIND_PUBLICATION_{user_project_id}_READ_ROLE"
        elif bind_role == 'bind_publicationdb_write_role':
            bind_name = f"BIND_PUBLICATION_{user_project_id}_WRITE_ROLE"
        bind_role = {
            "name": bind_name,
            **{k: v for k, v in bind_role_data.items() if k != 'user_project_id'}
        }
        data['role'] = bind_role
    
    # roleキーが存在するか確認
    role_data = data.get('role')
    if not role_data:
        print(f"【Warning】{file_path}にroleキーが存在しません")
        return

    # create_roleキーが存在するか、値がbooleanか確認
    create_role = role_data.get('create_role')
    if create_role is None or not isinstance(create_role, bool):
        print(f"【Warning】{file_path}にcreate_roleキーが存在しないか、その値がbooleanではありません")
        return

    database_priv = role_data.pop('database_priv', [])
    warehouse_priv = role_data.pop('warehouse_priv', [])
    resource_monitor_priv = role_data.pop('resource_monitor_priv', [])
    schema_priv = role_data.pop('schema_priv', [])
    schema_object_priv = role_data.pop('schema_object_priv', [])
    global_priv = role_data.pop('global_priv', [])
    parent_role = role_data.pop('parent_role', [])
    
     # ロールを生成
    role = [{
        "name": role_data.get("name"),
        "comment": role_data.get("comment")
    }] if create_role else []

    # アカウントオブジェクト権限をロールに付与
    priv_accobj = [
        {
            "role": role_data.get("name"),
            "object_type": "DATABASE",
            "object_name": entry.get("database_name"),
            "privileges": entry.get("priv_type")
        }
        for entry in database_priv
    ] + [
        {
            "role": role_data.get("name"),
            "object_type": "WAREHOUSE",
            "object_name": entry.get("warehouse_name"),
            "privileges": entry.get("priv_type")
        }
        for entry in warehouse_priv
    ] + [
        {
            "role": role_data.get("name"),
            "object_type": "RESOURCE MONITOR",
            "object_name": entry.get("resource_monitor_name"),
            "privileges": entry.get("priv_type")
        }
        for entry in resource_monitor_priv
    ]

    # スキーマ権限をロールに付与
    priv_schem = [
        {
            "role": role_data.get("name"),
            "schema": entry.get("schema_name"),
            "privileges": entry.get("priv_type")
        }
        for entry in schema_priv
    ]

    # スキーマ配下オブジェクト権限をロールに付与
    priv_schobj = []
    priv_schobj_all = []
    priv_schobj_futr = []
    priv_schobj_own = []
    priv_schobj_own_all = []
    priv_schobj_own_futr = []
    for entry in schema_object_priv:
        all_option = entry.get("all_option")
        future_option = entry.get("future_option")
        # all/future_optionキーが存在するか確認
        if all_option or future_option:
            # all/future_optionキーの値がbooleanか確認
            if not isinstance(all_option, bool) or not isinstance(future_option, bool):
                print(f"【Warning】{file_path}のall/future_optionキーがbooleanではありません")
                return

            priv_schobj_af = [
                {
                    "role": role_data.get("name"),
                    "schema": entry.get("schema_name"),
                    "object_type": object_type,
                    "privileges": [priv for priv in entry.get("priv_type", []) if priv != "OWNERSHIP"]
                }
                for object_type in entry.get("object_type", [])
            ]
            # "OWNERSHIP"が含まれている場合、priv_schobj_ownに追加
            priv_schobj_own_af = []
            if "OWNERSHIP" in entry.get("priv_type", []):
                priv_schobj_own_af = [
                    {
                        "role": role_data.get("name"),
                        "schema": entry.get("schema_name"),
                        "object_type": object_type
                    }
                    for object_type in entry.get("object_type", [])
                ]
            if all_option:
                priv_schobj_all.extend(priv_schobj_af)
                if priv_schobj_own_af:
                    priv_schobj_own_all.extend(priv_schobj_own_af)
            if future_option:
                priv_schobj_futr.extend(priv_schobj_af)
                if priv_schobj_own_af:
                    priv_schobj_own_futr.extend(priv_schobj_own_af)

        # object_nameキーが存在するか確認
        object_name = entry.get("object_name")
        if object_name:
            priv_schobj.append({
                "role": role_data.get("name"),
                "object_type": entry.get("object_type"),
                "object_name": entry.get("object_name"),
                "privileges": [priv for priv in entry.get("priv_type", []) if priv != "OWNERSHIP"]
            })
            # "OWNERSHIP"が含まれている場合、priv_schobj_ownに追加
            if "OWNERSHIP" in entry.get("priv_type", []):
                priv_schobj_own.append({
                "role": role_data.get("name"),
                "object_type": entry.get("object_type"),
                "object_name": entry.get("object_name")
            })

    # グローバル権限をロールに付与
    priv_global = [{
        "role": role_data.get("name"),
        "privileges": global_priv
    }] if global_priv else []

    # 親ロールをロールに付与
    priv_role = [
        {
            "role": role_data.get("name"),
            "parent_role": entry
        }
        for entry in parent_role
    ]

    # システムロールの定型設定を追加
    if system_name is not None:
        priv_accobj.append({
            "role": role_data.get("name"),
            "object_type": "WAREHOUSE",
            "object_name": f"{system_name}_WH",
            "privileges": ["USAGE"]
        })

    # BINDロールの定型設定を追加
    if database_name is not None or schema_name is not None:
        priv_accobj.append({
            "role": role_data.get("name"),
            "object_type": "DATABASE",
            "object_name": database_name,
            "privileges": ["USAGE"]
        })
        priv_schem.append({
            "role": role_data.get("name"),
            "schema": f"{database_name}.{schema_name}",
            "privileges": ["USAGE"]
        })
        bind_priv_schobj_af = [
            {
                "role": role_data.get("name"),
                "schema": f"{database_name}.{schema_name}",
                "object_type": object_type,
                "privileges": ["SELECT"]
            }
            for object_type in ["TABLES", "VIEWS", "MATERIALIZED VIEWS"]
        ]
        priv_schobj_all.extend(bind_priv_schobj_af)
        priv_schobj_futr.extend(bind_priv_schobj_af)

    # ユーザ/公開領域WRITE/READロールの定型設定を追加
    if user_project_id is not None:
        if (bind_role := next((r for r in bind_roles if r in data), None)): 
            if bind_role == 'bind_userdb_read_role':
                priv_accobj.append({
                    "role": role_data.get("name"),
                    "object_type": "DATABASE",
                    "object_name": "USER_PROJECT_DB",
                    "privileges": ["USAGE"]
                })
                priv_accobj.append({
                    "role": role_data.get("name"),
                    "object_type": "WAREHOUSE",
                    "object_name": f"USER_{user_project_id}_WH",
                    "privileges": ["USAGE"]
                })
                priv_schem.append({
                    "role": role_data.get("name"),
                    "schema": f"USER_PROJECT_DB.USER_{user_project_id}_SCHEMA",
                    "privileges": ["USAGE"]
                })
                user_bind_priv_schobj_af = [
                    {
                        "role": role_data.get("name"),
                        "schema": f"USER_PROJECT_DB.USER_{user_project_id}_SCHEMA",
                        "object_type": object_type,
                        "privileges": ["SELECT"]
                    }
                    for object_type in ["TABLES", "VIEWS", "MATERIALIZED VIEWS"]
                ]
                priv_schobj_all.extend(user_bind_priv_schobj_af)
                priv_schobj_futr.extend(user_bind_priv_schobj_af)
                user_bind_priv_role = [
                    {
                        "role": role_data.get("name"),
                        "parent_role": entry
                    }
                    for entry in ["BIND_USER_PROJECT_DB_READ_ROLE", f"BIND_USER_{user_project_id}_WRITE_ROLE", f"BIND_PUBLICATION_{user_project_id}_WRITE_ROLE"] 
                ]
                priv_role.extend(user_bind_priv_role)
            elif bind_role == 'bind_userdb_write_role':
                priv_accobj.append({
                    "role": role_data.get("name"),
                    "object_type": "DATABASE",
                    "object_name": "USER_PROJECT_DB",
                    "privileges": ["USAGE"]
                })
                priv_accobj.append({
                    "role": role_data.get("name"),
                    "object_type": "WAREHOUSE",
                    "object_name": f"USER_{user_project_id}_WH",
                    "privileges": ["USAGE", "MONITOR", "OPERATE"]
                })
                priv_accobj.append({
                    "role": role_data.get("name"),
                    "object_type": "RESOURCE MONITOR",
                    "object_name": f"USER_{user_project_id}_RM",
                    "privileges": ["MONITOR"]
                })
                priv_schem.append({
                    "role": role_data.get("name"),
                    "schema": f"USER_PROJECT_DB.USER_{user_project_id}_SCHEMA",
                    "privileges": ["USAGE", "CREATE TABLE", "CREATE VIEW", "CREATE MATERIALIZED VIEW", "CREATE FILE FORMAT", "CREATE FUNCTION", "CREATE STREAM", "CREATE TASK", "CREATE PROCEDURE"]
                })
                user_bind_priv_schobj_own_af = [
                    {
                        "role": role_data.get("name"),
                        "schema": f"USER_PROJECT_DB.USER_{user_project_id}_SCHEMA",
                        "object_type": object_type,
                    }
                    for object_type in ["TABLES", "VIEWS", "MATERIALIZED VIEWS", "FILE FORMATS", "FUNCTIONS", "STREAMS", "TASKS", "PROCEDURES"] 
                ]
                priv_schobj_own_futr.extend(user_bind_priv_schobj_own_af)
                priv_global.append({
                    "role": role_data.get("name"),
                    "privileges": ["EXECUTE TASK"]
                })
                priv_role.append({
                    "role": role_data.get("name"),
                    "parent_role": "SYSADMIN"
                })
            elif bind_role == 'bind_publicationdb_read_role':
                priv_accobj.append({
                    "role": role_data.get("name"),
                    "object_type": "DATABASE",
                    "object_name": "USER_PUBLICATION_DB",
                    "privileges": ["USAGE"]
                })
                priv_role.append({
                    "role": role_data.get("name"),
                    "parent_role": f"BIND_PUBLICATION_{user_project_id}_WRITE_ROLE"
                })
            elif bind_role == 'bind_publicationdb_write_role':
                priv_accobj.append({
                    "role": role_data.get("name"),
                    "object_type": "DATABASE",
                    "object_name": "USER_PUBLICATION_DB",
                    "privileges": ["USAGE"]
                })
                priv_role.append({
                    "role": role_data.get("name"),
                    "parent_role": "SYSADMIN"
                })

    converted_data = {}
    if role:
        converted_data["role"] = role
    if priv_accobj:
        converted_data["priv_accobj"] = priv_accobj
    if priv_schem:
        converted_data["priv_schem"] = priv_schem
    if priv_schobj_all:
        converted_data["priv_schobj_all"] = priv_schobj_all
    if priv_schobj_futr:
        converted_data["priv_schobj_futr"] = priv_schobj_futr
    if priv_schobj_own_all:
        converted_data["priv_schobj_own_all"] = priv_schobj_own_all
    if priv_schobj_own_futr:
        converted_data["priv_schobj_own_futr"] = priv_schobj_own_futr
    if priv_schobj_own:
        converted_data["priv_schobj_own"] = priv_schobj_own
    if priv_schobj:
        converted_data["priv_schobj"] = priv_schobj
    if priv_global:
        converted_data["priv_global"] = priv_global
    if priv_role:
        converted_data["priv_role"] = priv_role

    save_yml(file_path, converted_data)
    print(f"【Success】{file_path}の変換が完了しました")

# DBロール生成
def transform_db_role_yml(file_path):
    data = load_yml(file_path)

    # db_roleキーが存在するか確認
    db_role_data = data.get('database_role')
    if not db_role_data:
        print(f"【Warning】{file_path}にdatabase_roleキーが存在しません")
        return

    # create_roleキーが存在するか、値がbooleanか確認
    create_role = db_role_data.get('create_role')
    if create_role is None or not isinstance(create_role, bool):
        print(f"【Warning】{file_path}にcreate_roleキーが存在しないか、その値がbooleanではありません")
        return

    database_priv = db_role_data.pop('database_priv', [])
    schema_priv = db_role_data.pop('schema_priv', [])
    schema_object_priv = db_role_data.pop('schema_object_priv', [])
    parent_role = db_role_data.pop('parent_role', [])

    # DBロールを生成
    db_role = [{
        "name": db_role_data.get("name"),
        "database": db_role_data.get("database_name"),
        "comment": db_role_data.get("comment")
    }] if create_role else []

    # データベース権限をDBロールに付与
    dr_priv_accobj = [
        {
            "db_role": f"{db_role_data.get('database_name')}.{db_role_data.get('name')}",
            "database": entry.get("database_name"),
            "privileges": entry.get("priv_type")
        }
        for entry in database_priv
    ]

    # スキーマ権限をDBロールに付与
    dr_priv_schem = [
        {
            "db_role": f"{db_role_data.get('database_name')}.{db_role_data.get('name')}",
            "schema": entry.get("schema_name"),
            "privileges": entry.get("priv_type")
        }
        for entry in schema_priv
    ]

    # スキーマ配下オブジェクト権限をDBロールに付与
    dr_priv_schobj = []
    dr_priv_schobj_all = []
    dr_priv_schobj_futr = []
    for entry in schema_object_priv:
        all_option = entry.get("all_option")
        future_option = entry.get("future_option")
        if all_option is not None or future_option is not None:
            if not (isinstance(all_option, bool) and isinstance(future_option, bool)):
                print(f"【Warning】{file_path}のall/future_optionキーがbooleanではありません")
                return

            dr_priv_schobj_af = [
                {
                    "db_role": f"{db_role_data.get('database_name')}.{db_role_data.get('name')}",
                    "schema": entry.get("schema_name"),
                    "object_type": object_type,
                    "privileges": entry.get("priv_type")
                }
                for object_type in entry.get("object_type", [])
            ]
            if all_option:
                dr_priv_schobj_all.extend(dr_priv_schobj_af)
            if future_option:
                dr_priv_schobj_futr.extend(dr_priv_schobj_af)

        object_name = entry.get("object_name")
        if object_name is not None:
            dr_priv_schobj.append({
                "db_role": f"{db_role_data.get('database_name')}.{db_role_data.get('name')}",
                "object_type": entry.get("object_type"),
                "object_name": entry.get("object_name"),
                "privileges": entry.get("priv_type")
            })

    # 親ロールをDBロールに付与
    priv_db_role = [
        {
            "db_role": db_role_data.get("name"),
            "database": db_role_data.get("database_name"),
            "parent_role": entry
        }
        for entry in parent_role
    ]

    converted_data = {}
    if db_role:
        converted_data["db_role"] = db_role
    if dr_priv_accobj:
        converted_data["dr_priv_accobj"] = dr_priv_accobj
    if dr_priv_schem:
        converted_data["dr_priv_schem"] = dr_priv_schem
    if dr_priv_schobj_all:
        converted_data["dr_priv_schobj_all"] = dr_priv_schobj_all
    if dr_priv_schobj_futr:
        converted_data["dr_priv_schobj_futr"] = dr_priv_schobj_futr
    if dr_priv_schobj:
        converted_data["dr_priv_schobj"] = dr_priv_schobj
    if priv_db_role:
        converted_data["priv_db_role"] = priv_db_role

    save_yml(file_path, converted_data)
    print(f"【Success】{file_path}の変換が完了しました")

# ユーザyml生成
def transform_user_yml(file_path):
    data = load_yml(file_path)

    # system_userキーが存在する場合、それを基にuserキーを生成
    system_name = None
    if 'system_user' in data:
        system_user_data = data.pop('system_user')
        system_name = system_user_data.get('system_name')
        if system_name is None:
            print(f"【Warning】{file_path}のsystem_userキーにsystem_nameキーが存在しません")
            return
        system_user = {
            "name": f"{system_name}_USER",
            "default_warehouse": f"{system_name}_WH",
            "default_role": f"{system_name}_ROLE",
            **{k: v for k, v in system_user_data.items() if k != 'system_name'}
        }
        data['user'] = system_user

    # userキーが存在するか確認
    user_data = data.get('user')
    if not user_data:
        print(f"【Warning】{file_path}にuserキーが存在しません")
        return

    # create_userキーが存在するか、値がbooleanか確認
    create_user = user_data.get('create_user')
    if create_user is None or not isinstance(create_user, bool):
        print(f"【Warning】{file_path}にcreate_userキーが存在しないか、その値がbooleanではありません")
        return

    granted_role = user_data.pop('granted_role', [])

    # ユーザを生成
    user = [{
        "name": user_data.get("name"),
        "default_warehouse": user_data.get("default_warehouse"),
        "default_namespace": user_data.get("default_namespace"),
        "default_role": user_data.get("default_role"),
        "default_secondary_roles_option": user_data.get("default_secondary_roles"),
        "rsa_public_key": user_data.get("rsa_public_key"),
        "rsa_public_key_2": user_data.get("rsa_public_key_2"),
        "network_policy": user_data.get("network_policy"),
        "statement_timeout_in_seconds": user_data.get("statement_timeout_in_seconds"),
        "client_session_keep_alive": user_data.get("client_session_keep_alive"),
        "comment": user_data.get("comment")
    }] if create_user else []

    # ロールをユーザに付与
    priv_role_user = [
        {"role": entry, "user": user_data.get("name")}
        for entry in granted_role
    ]

    # システムユーザの定型設定を追加
    if system_name:
        priv_role_user.append({
            "role": f"{system_name}_ROLE",
            "user": user_data.get("name")
        })

    converted_data = {}
    if user:
        converted_data["user"] = user
    if priv_role_user:
        converted_data["priv_role_user"] = priv_role_user
    save_yml(file_path, converted_data)
    print(f"【Success】{file_path}の変換が完了しました")

# ファイル読込
def load_yml(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

# ファイル保存
def save_yml(file_path, data):
    with open(file_path, 'w', encoding='utf-8') as f:
        yaml.dump(data, f, allow_unicode=True, sort_keys=False)

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    yml_dir = os.path.join(base_dir, "..", "..", "yml")

    role_files = glob.glob(os.path.join(yml_dir, "role", '*.yml'))
    db_role_files = glob.glob(os.path.join(yml_dir, "database_role", '*.yml'))
    user_files = glob.glob(os.path.join(yml_dir, "user", '*.yml'))

    [transform_role_yml(file) for file in role_files]
    [transform_db_role_yml(file) for file in db_role_files]
    [transform_user_yml(file) for file in user_files]

if __name__ == "__main__":  
  main()