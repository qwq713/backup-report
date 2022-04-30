from openpyxl import Workbook
from datetime import datetime

def make_workbook():
    return Workbook()

def attach_sheet(workbook, sheet_name ,header, rows):
    header_len = len(header)
    for row in rows:
        if len(row) != header_len:
            raise Exception(f"""
                            header의 길이와 row의 길이가 다릅니다.
                            문제가 있는 row : {row}
                            """)
    
    data_list = []
    data_list.append(header)
    data_list.extend(rows)
    
    sheet = workbook.create_sheet()
    sheet.title = sheet_name
    
    for data in data_list:
        sheet.append(data)
    
    return workbook

def make_excel_file(workbook,account_id):
    dt = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    workbook.save(filename=f"./{account_id}_backup_report_{dt}.xlsx")
    return f"./backup_report_{dt}.xlsx"