import sqlparse
import sys
import traceback


if __name__ == "__main__":
    if len(sys.argv) > 1:
        sourceSQL = str(sys.argv[0])
        encoding = str(sys.argv[1])

    # DEBUG
    sourceSQL = "./test-files/EI_znamky_2F_a_3F__utf8.sql"
    encoding = "utf-8"
    # sourceSQL = "./test-files/Plany_prerekvizity_kontrola__utf8.sql"
    # encoding = "utf-8"
    # sourceSQL = "./test-files/Plany_prerekvizity_kontrola__ansi.sql"
    # encoding = "ansi"

    try:
        with open(sourceSQL, mode="r", encoding=encoding) as file:
            query = "".join(file.readlines())
        
        # VIZ:
        #   * https://stackoverflow.com/questions/22303812/how-to-parse-sql-queries-and-sub-queries-using-sqlparser-into-python
        #   * https://stackoverflow.com/questions/72087411/simple-way-to-parse-sql-subqueries
        
        statements = sqlparse.parse(query, encoding=encoding)

            

        # print(statements)
        

    except:
        print("\nDOŠLO K CHYBĚ:\n\n" + traceback.format_exc())
