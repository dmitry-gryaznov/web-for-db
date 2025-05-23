import datetime
import decimal
import json
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Form, HTTPException
from fastapi.requests import Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import (Date, DateTime, Integer, MetaData, Numeric, inspect,
                        text)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from database_manager import engine, get_db, get_table_names


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        return super().default(obj)


def custom_json_serializer(obj):
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def safe_jsonify(row):
    if not row:
        return "{}"

    safe_row = {}
    for key, value in row.items():
        if isinstance(value, (datetime.date, datetime.datetime)):
            safe_row[key] = value.isoformat()
        elif isinstance(value, decimal.Decimal):
            safe_row[key] = float(value)
        else:
            safe_row[key] = value

    return json.dumps(safe_row)


router = APIRouter()
templates = Jinja2Templates(directory="templates")
templates.env.filters["safe_json"] = safe_jsonify
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_table_data(table_name: str, db: Session):
    result = db.execute(text(f'SELECT * FROM "{table_name}"'))
    columns = list(result.keys())
    rows = result.fetchall()

    data = []
    for row in rows:
        row_dict = {}
        for i, column in enumerate(columns):
            value = row[i]
            if isinstance(value, (datetime.date, datetime.datetime)):
                row_dict[column] = value.isoformat()
            elif isinstance(value, decimal.Decimal):
                row_dict[column] = float(value)
            else:
                row_dict[column] = value
        data.append(row_dict)

    return columns, data


def get_table_info(table_name: str) -> Dict[str, Any]:
    metadata = MetaData()
    metadata.reflect(bind=engine)
    if table_name not in metadata.tables:
        raise HTTPException(status_code=404, detail=f"Таблица {table_name} не найдена")
    table = metadata.tables[table_name]
    primary_keys = [column.name for column in table.primary_key.columns]
    required_columns = [column.name for column in table.columns
                        if not column.nullable and column.default is None]
    return {
        "primary_keys": primary_keys,
        "required_columns": required_columns,
        "columns": [column.name for column in table.columns]
    }

@router.get("/view")
async def show_tables(
        request: Request,
        table_name: str = None,
        message: Optional[str] = None,
        message_type: Optional[str] = None,
        db: Session = Depends(get_db)
):
    print(f'{table_name=}')
    tables = get_table_names()
    columns = []
    data = []
    primary_keys = []
    required_columns = []
    if table_name and table_name in tables:
        columns, data = get_table_data(table_name, db)
        if table_name:
            table_info = get_table_info(table_name)
            primary_keys = table_info["primary_keys"]
            required_columns = table_info["required_columns"]
    return templates.TemplateResponse("view.html", {
        "request": request,
        "tables": tables,
        "current_table": table_name,
        "columns": columns,
        "data": data,
        "primary_keys": primary_keys,
        "required_columns": required_columns,
        "message": message,
        "message_type": message_type
    })

def execute_custom_query(query: str, db: Session):
    result = db.execute(text(query))
    columns = list(result.keys())
    rows = result.fetchall()
    data = []
    for row in rows:
        row_dict = {}
        for i, column in enumerate(columns):
            value = row[i]
            if isinstance(value, (datetime.date, datetime.datetime)):
                row_dict[column] = value.isoformat()
            elif isinstance(value, decimal.Decimal):
                row_dict[column] = float(value)
            else:
                row_dict[column] = value
        data.append(row_dict)

    return columns, data


@router.get("/query1")
async def run_query1(request: Request, db: Session = Depends(get_db)):
    query = """
    WITH ЗадолженностиКлиентов AS (
        SELECT 
            п.лицевой_счет,
            COUNT(DISTINCT п.код_услуги) AS количество_неоплаченных_услуг,
            SUM(п.сумма_к_оплате - COALESCE(пл.сумма_платежа, 0)) AS общая_задолженность
        FROM 
            потребление п
        LEFT JOIN 
            платежи пл ON п.лицевой_счет = пл.лицевой_счет 
            AND п.код_услуги = пл.код_услуги 
            AND п.период = пл.период
        WHERE 
            (п.сумма_к_оплате - COALESCE(пл.сумма_платежа, 0)) > 0
            OR пл.сумма_платежа IS NULL
        GROUP BY 
            п.лицевой_счет
        HAVING 
            COUNT(DISTINCT п.код_услуги) > 1
    )
    SELECT 
        з.лицевой_счет,
        к.фио,
        к.адрес,
        к.телефон,
        з.количество_неоплаченных_услуг,
        з.общая_задолженность
    FROM 
        ЗадолженностиКлиентов з
    JOIN 
        клиенты к ON з.лицевой_счет = к.лицевой_счет
    ORDER BY 
        з.общая_задолженность DESC
    """

    columns, data = execute_custom_query(query, db)

    return templates.TemplateResponse("view.html", {
        "request": request,
        "tables": get_table_names(),
        "current_query": "query1",
        "columns": columns,
        "data": data,
        "query_title": "Клиенты с задолженностью по более чем 1 услуге"
    })


@router.get("/query2")
async def run_query2(request: Request, db: Session = Depends(get_db)):
    query = """
    SELECT 
        у.код_услуги,
        у.наименование AS услуга,
        у.единица_измерения,
        т.тарифная_зона,
        т.стоимость_единицы AS текущий_тариф,
        т.действует_с,
        т.действует_по,
        ROUND(AVG(п.объем), 2) AS средний_объем_потребления,
        COUNT(DISTINCT п.лицевой_счет) AS количество_потребителей
    FROM 
        услуги у
    JOIN 
        тарифы т ON у.код_услуги = т.код_услуги
    LEFT JOIN 
        потребление п ON у.код_услуги = п.код_услуги
    WHERE 
        (т.действует_по IS NULL OR т.действует_по >= CURRENT_DATE)
        AND т.действует_с <= CURRENT_DATE
    GROUP BY 
        у.код_услуги, у.наименование, у.единица_измерения, 
        т.тарифная_зона, т.стоимость_единицы, т.действует_с, т.действует_по
    ORDER BY 
        у.наименование, т.тарифная_зона
    """

    columns, data = execute_custom_query(query, db)

    return templates.TemplateResponse("view.html", {
        "request": request,
        "tables": get_table_names(),
        "current_query": "query2",
        "columns": columns,
        "data": data,
        "query_title": "Коммунальные услуги с тарифами и объемами потребления"
    })


@router.get("/query3")
async def run_query3(request: Request, db: Session = Depends(get_db)):
    query = """
    WITH ПоследнийМесяц AS (
        SELECT MAX(период) AS макс_период FROM потребление
    ),
    СреднееПотребление AS (
        SELECT 
            п.код_услуги,
            у.наименование,
            AVG(п.объем) AS среднее_значение
        FROM 
            потребление п
        JOIN 
            услуги у ON п.код_услуги = у.код_услуги
        JOIN 
            ПоследнийМесяц пм ON п.период = пм.макс_период
        GROUP BY 
            п.код_услуги, у.наименование
    )
    SELECT 
        к.лицевой_счет,
        к.фио,
        к.адрес,
        у.наименование AS услуга,
        п.период,
        п.объем AS потребление_клиента,
        с.среднее_значение AS среднее_потребление,
        ROUND((п.объем - с.среднее_значение) / с.среднее_значение * 100, 2) AS процент_превышения
    FROM 
        потребление п
    JOIN 
        клиенты к ON п.лицевой_счет = к.лицевой_счет
    JOIN 
        услуги у ON п.код_услуги = у.код_услуги
    JOIN 
        СреднееПотребление с ON п.код_услуги = с.код_услуги
    JOIN 
        ПоследнийМесяц пм ON п.период = пм.макс_период
    WHERE 
        п.объем > с.среднее_значение
    ORDER BY 
        у.наименование, процент_превышения DESC
    """

    columns, data = execute_custom_query(query, db)
    return templates.TemplateResponse("view.html", {
        "request": request,
        "tables": get_table_names(),
        "current_query": "query3",
        "columns": columns,
        "data": data,
        "query_title": "Клиенты с потреблением выше среднего за последний месяц"
    })

@router.get("/query4")
async def run_query4(request: Request, db: Session = Depends(get_db)):
    query = """
    WITH ПоследнийМесяц AS (
        SELECT MAX(период) AS макс_период FROM платежи
    )
    SELECT 
        к.лицевой_счет,
        к.фио,
        к.адрес,
        у.наименование AS услуга,
        SUM(пл.сумма_платежа) AS сумма_оплаты
    FROM 
        платежи пл
    JOIN 
        клиенты к ON пл.лицевой_счет = к.лицевой_счет
    JOIN 
        услуги у ON пл.код_услуги = у.код_услуги
    JOIN 
        ПоследнийМесяц пм ON пл.период = пм.макс_период
    WHERE 
        к.адрес LIKE '%ул. Пушкина%'
    GROUP BY 
        к.лицевой_счет, к.фио, к.адрес, у.наименование
    ORDER BY 
        к.фио, у.наименование
    """

    columns, data = execute_custom_query(query, db)

    return templates.TemplateResponse("view.html", {
        "request": request,
        "tables": get_table_names(),
        "current_query": "query4",
        "columns": columns,
        "data": data,
        "query_title": "Ведомость оплаты коммунальных услуг по адресу 'ул. Пушкина'"
    })

@router.get("/query5")
async def run_query5(request: Request, db: Session = Depends(get_db)):
    query = """
    WITH МесяцыТекущегоГода AS (
        SELECT 
            DISTINCT период 
        FROM 
            потребление 
        WHERE 
            EXTRACT(YEAR FROM период) = EXTRACT(YEAR FROM CURRENT_DATE)
        ORDER BY 
            период
    ),
    ЗадолженностьПоУслугам AS (
        SELECT 
            п.период,
            п.код_услуги,
            у.наименование AS услуга,
            SUM(п.сумма_к_оплате) AS начислено,
            SUM(COALESCE(пл.сумма_платежа, 0)) AS оплачено,
            SUM(п.сумма_к_оплате - COALESCE(пл.сумма_платежа, 0)) AS задолженность,
            COUNT(DISTINCT CASE WHEN (п.сумма_к_оплате - COALESCE(пл.сумма_платежа, 0)) > 0 
                              THEN п.лицевой_счет END) AS количество_должников
        FROM 
            потребление п
        JOIN 
            услуги у ON п.код_услуги = у.код_услуги
        LEFT JOIN 
            платежи пл ON п.лицевой_счет = пл.лицевой_счет 
            AND п.код_услуги = пл.код_услуги 
            AND п.период = пл.период
        WHERE 
            EXTRACT(YEAR FROM п.период) = EXTRACT(YEAR FROM CURRENT_DATE)
        GROUP BY 
            п.период, п.код_услуги, у.наименование
    )
    SELECT 
        TO_CHAR(з.период, 'Month YYYY') AS месяц,
        з.услуга,
        з.начислено,
        з.оплачено,
        з.задолженность,
        з.количество_должников,
        ROUND(з.задолженность / NULLIF(з.начислено, 0) * 100, 2) AS процент_задолженности
    FROM 
        ЗадолженностьПоУслугам з
    JOIN 
        МесяцыТекущегоГода м ON з.период = м.период
    ORDER BY 
        з.период, з.услуга
    """

    columns, data = execute_custom_query(query, db)

    return templates.TemplateResponse("view.html", {
        "request": request,
        "tables": get_table_names(),
        "current_query": "query5",
        "columns": columns,
        "data": data,
        "query_title": "Задолженность по коммунальным услугам по месяцам"
    })


@router.get("/api/tables/info")
async def get_tables_info(db: Session = Depends(get_db)):
    try:
        tables_info = {}
        table_names = get_table_names()

        for table_name in table_names:
            metadata = MetaData()
            metadata.reflect(bind=engine)

            if table_name in metadata.tables:
                table = metadata.tables[table_name]
                columns_info = []

                for column in table.columns:
                    columns_info.append({
                        "name": column.name,
                        "type": str(column.type),
                        "nullable": column.nullable,
                        "primary_key": column.primary_key
                    })

                tables_info[table_name] = {
                    "columns": columns_info,
                    "primary_keys": [col.name for col in table.primary_key.columns]
                }

        return tables_info

    except Exception as e:
        logger.error(f"Ошибка при получении информации о таблицах: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения информации о таблицах: {str(e)}")


@router.get("/sql_editor")
async def show_sql_editor(
        request: Request,
        db: Session = Depends(get_db)
):
    return templates.TemplateResponse("view.html", {
        "request": request,
        "tables": get_table_names(),
        "current_query": "custom_sql",
        "columns": [],
        "data": [],
        "sql_query": "",
        "sql_error": None,
        "sql_success": None
    })


@router.post("/execute_sql")
async def execute_sql(
        request: Request,
        sql_query: str = Form(...),
        db: Session = Depends(get_db)
):
    try:
        sql_query = sql_query.strip()

        if not sql_query:
            return templates.TemplateResponse("view.html", {
                "request": request,
                "tables": get_table_names(),
                "current_query": "custom_sql",
                "columns": [],
                "data": [],
                "sql_query": sql_query,
                "sql_error": "Запрос не может быть пустым",
                "sql_success": None
            })

        query_type = sql_query.upper().strip().split()[0]

        if query_type == "SELECT":
            columns, data = execute_custom_query(sql_query, db)

            success_message = f"Запрос выполнен успешно. Найдено записей: {len(data)}"

            return templates.TemplateResponse("view.html", {
                "request": request,
                "tables": get_table_names(),
                "current_query": "custom_sql",
                "columns": columns,
                "data": data,
                "sql_query": sql_query,
                "sql_error": None,
                "sql_success": success_message
            })

        elif query_type in ["INSERT", "UPDATE", "DELETE"]:
            result = db.execute(text(sql_query))
            db.commit()
            rows_affected = result.rowcount

            success_message = f"Запрос выполнен успешно. Затронуто строк: {rows_affected}"

            return templates.TemplateResponse("view.html", {
                "request": request,
                "tables": get_table_names(),
                "current_query": "custom_sql",
                "columns": [],
                "data": [],
                "sql_query": sql_query,
                "sql_error": None,
                "sql_success": success_message
            })

        else:
            dangerous_keywords = ["DROP", "CREATE", "ALTER", "TRUNCATE", "GRANT", "REVOKE"]
            if query_type in dangerous_keywords:
                return templates.TemplateResponse("view.html", {
                    "request": request,
                    "tables": get_table_names(),
                    "current_query": "custom_sql",
                    "columns": [],
                    "data": [],
                    "sql_query": sql_query,
                    "sql_error": f"Выполнение запросов типа {query_type} ограничено из соображений безопасности",
                    "sql_success": None
                })

            db.execute(text(sql_query))
            db.commit()

            return templates.TemplateResponse("view.html", {
                "request": request,
                "tables": get_table_names(),
                "current_query": "custom_sql",
                "columns": [],
                "data": [],
                "sql_query": sql_query,
                "sql_error": None,
                "sql_success": "Запрос выполнен успешно"
            })

    except SQLAlchemyError as e:
        logger.error(f"Ошибка при выполнении SQL запроса: {str(e)}")
        db.rollback()

        error_message = str(e).split('\n')[0]
        if "DETAIL:" in str(e):
            detail_part = str(e).split("DETAIL:")[1].split('\n')[0].strip()
            error_message += f" Детали: {detail_part}"

        return templates.TemplateResponse("view.html", {
            "request": request,
            "tables": get_table_names(),
            "current_query": "custom_sql",
            "columns": [],
            "data": [],
            "sql_query": sql_query,
            "sql_error": error_message,
            "sql_success": None
        })

    except Exception as e:
        logger.error(f"Неожиданная ошибка при выполнении SQL запроса: {str(e)}")

        return templates.TemplateResponse("view.html", {
            "request": request,
            "tables": get_table_names(),
            "current_query": "custom_sql",
            "columns": [],
            "data": [],
            "sql_query": sql_query,
            "sql_error": f"Неожиданная ошибка: {str(e)}",
            "sql_success": None
        })

@router.post("/add_record")
async def add_record(
        request: Request,
        table_name: str = Form(...),
        db: Session = Depends(get_db)
):
    try:
        form_data = await request.form()

        form_dict = dict(form_data)
        del form_dict["table_name"]

        if table_name not in get_table_names():
            raise HTTPException(status_code=404, detail=f"Таблица {table_name} не найдена")

        metadata = MetaData()
        metadata.reflect(bind=engine)
        table = metadata.tables[table_name]

        for column in table.columns:
            if column.name in form_dict:
                value = form_dict[column.name]

                if value == "" and column.nullable:
                    form_dict[column.name] = None
                    continue

                if isinstance(column.type, (Date, DateTime)) and value:
                    try:
                        if isinstance(column.type, Date):
                            form_dict[column.name] = datetime.date.fromisoformat(value)
                        elif isinstance(column.type, DateTime):
                            form_dict[column.name] = datetime.datetime.fromisoformat(value)
                    except ValueError:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Неверный формат даты для поля {column.name}. Используйте формат YYYY-MM-DD."
                        )

                if isinstance(column.type, (Integer, Numeric)) and value:
                    try:
                        if isinstance(column.type, Integer):
                            form_dict[column.name] = int(value)
                        elif isinstance(column.type, Numeric):
                            form_dict[column.name] = float(value)
                    except ValueError:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Неверный числовой формат для поля {column.name}."
                        )

        columns = ", ".join([f'"{key}"' for key in form_dict.keys()])
        placeholders = ", ".join([f":{key}" for key in form_dict.keys()])

        query = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders})'

        db.execute(text(query), form_dict)
        db.commit()

        logger.info(f"Запись успешно добавлена в таблицу {table_name}")

        return RedirectResponse(
            url=f"/view?table_name={table_name}&message=Запись успешно добавлена&message_type=success",
            status_code=303
        )
    except SQLAlchemyError as e:
        logger.error(f"Ошибка при добавлении записи: {str(e)}")
        db.rollback()
        return RedirectResponse(
            url=f"/view?table_name={table_name}&message=Ошибка при добавлении записи: {str(e)}&message_type=danger",
            status_code=303
        )


@router.post("/edit_record")
async def edit_record(
        request: Request,
        table_name: str = Form(...),
        db: Session = Depends(get_db)
):
    try:
        form_data = await request.form()
        form_dict = dict(form_data)

        del form_dict["table_name"]

        if table_name not in get_table_names():
            raise HTTPException(status_code=404, detail=f"Таблица {table_name} не найдена")

        primary_keys = {}
        for key, value in list(form_dict.items()):
            if key.startswith("pk_"):
                original_key = key[3:]
                primary_keys[original_key] = value
                del form_dict[key]

        for key, value in list(form_dict.items()):
            if value == "":
                form_dict[key] = None

        set_clause = ", ".join([f'"{key}" = :{key}' for key in form_dict.keys()])

        where_clause = " AND ".join([f'"{key}" = :{key}_pk' for key in primary_keys.keys()])
        where_values = {f"{key}_pk": value for key, value in primary_keys.items()}

        query_params = {**form_dict, **where_values}

        query = f'UPDATE "{table_name}" SET {set_clause} WHERE {where_clause}'

        db.execute(text(query), query_params)
        db.commit()

        logger.info(f"Запись успешно обновлена в таблице {table_name}")

        return RedirectResponse(
            url=f"/view?table_name={table_name}&message=Запись успешно обновлена&message_type=success",
            status_code=303
        )
    except SQLAlchemyError as e:
        logger.error(f"Ошибка при обновлении записи: {str(e)}")
        db.rollback()
        return RedirectResponse(
            url=f"/view?table_name={table_name}&message=Ошибка при обновлении записи: {str(e)}&message_type=danger",
            status_code=303
        )


@router.post("/delete_record")
async def delete_record(
        request: Request,
        table_name: str = Form(...),
        db: Session = Depends(get_db)
):
    try:
        form_data = await request.form()
        form_dict = dict(form_data)

        del form_dict["table_name"]

        if table_name not in get_table_names():
            raise HTTPException(status_code=404, detail=f"Таблица {table_name} не найдена")

        primary_keys = {}
        for key, value in form_dict.items():
            if key.startswith("pk_"):
                original_key = key[3:]
                primary_keys[original_key] = value

        where_clause = " AND ".join([f'"{key}" = :{key}' for key in primary_keys.keys()])

        query_params = {key: value for key, value in primary_keys.items()}

        query = f'DELETE FROM "{table_name}" WHERE {where_clause}'

        db.execute(text(query), query_params)
        db.commit()

        logger.info(f"Запись успешно удалена из таблицы {table_name}")

        return RedirectResponse(
            url=f"/view?table_name={table_name}&message=Запись успешно удалена&message_type=success",
            status_code=303
        )
    except SQLAlchemyError as e:
        logger.error(f"Ошибка при удалении записи: {str(e)}")
        db.rollback()
        return RedirectResponse(
            url=f"/view?table_name={table_name}&message=Ошибка при удалении записи: {str(e)}&message_type=danger",
            status_code=303
        )


@router.get("/api/tables/{table_name}/schema")
async def get_table_schema(table_name: str, db: Session = Depends(get_db)):
    try:
        if table_name not in get_table_names():
            raise HTTPException(status_code=404, detail=f"Таблица {table_name} не найдена")

        table_info = get_table_info(table_name)

        metadata = MetaData()
        metadata.reflect(bind=engine)

        if table_name not in metadata.tables:
            raise HTTPException(status_code=404, detail=f"Таблица {table_name} не найдена")

        table = metadata.tables[table_name]

        inspector = inspect(engine)
        foreign_keys = inspector.get_foreign_keys(table_name)

        columns_info = []
        for column in table.columns:
            column_type = str(column.type)
            input_type = "text"

            if "int" in column_type.lower():
                input_type = "number"
            elif "date" in column_type.lower():
                input_type = "date"
            elif "time" in column_type.lower():
                input_type = "time"
            elif "numeric" in column_type.lower():
                input_type = "number"

            is_foreign_key = False
            foreign_key_info = None

            for fk in foreign_keys:
                if column.name in fk['constrained_columns']:
                    is_foreign_key = True

                    target_table = fk['referred_table']
                    target_column = fk['referred_columns'][0]

                    query = f'SELECT DISTINCT "{target_column}" FROM "{target_table}"'
                    result = db.execute(text(query))
                    values = [row[0] for row in result.fetchall()]

                    foreign_key_info = {
                        "target_table": target_table,
                        "target_column": target_column,
                        "values": values
                    }
                    break

            columns_info.append({
                "name": column.name,
                "type": column_type,
                "input_type": input_type,
                "is_primary_key": column.primary_key,
                "is_nullable": column.nullable,
                "has_default": column.default is not None,
                "is_foreign_key": is_foreign_key,
                "foreign_key_info": foreign_key_info
            })

        return {
            "table_name": table_name,
            "primary_keys": table_info["primary_keys"],
            "required_columns": table_info["required_columns"],
            "columns": columns_info
        }
    except SQLAlchemyError as e:
        logger.error(f"Ошибка при получении схемы таблицы: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(e)}")