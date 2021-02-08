import psycopg2
import os
import sys


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS " + ratingstablename)
    cur.execute(
        "CREATE TABLE " + ratingstablename + " (UserID INT, temp1 VARCHAR(10),  MovieID INT , temp3 VARCHAR(10),  Rating REAL, temp5 VARCHAR(10), Timestamp INT)")
    cur.execute("CREATE TABLE META_DATA (key varchar(10),value varchar(30))");
    loadout = open(ratingsfilepath, 'r')
    cur.copy_from(loadout, ratingstablename, sep=':',
                  columns=('UserID', 'temp1', 'MovieID', 'temp3', 'Rating', 'temp5', 'Timestamp'))
    cur.execute(
        "ALTER TABLE " + ratingstablename + " DROP COLUMN temp1, DROP COLUMN temp3,DROP COLUMN temp5, DROP COLUMN Timestamp")
    cur.close()
    openconnection.commit()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    RangePart = numberofpartitions
    cur.execute("INSERT INTO META_DATA (key,value) VALUES ('RangePart'," + str(numberofpartitions) + ")")
    Range_consider = 5.0 / numberofpartitions
    table_sequence = 0
    Range = 0
    while Range < 5.0:
        if Range == 0:
            cur.execute("DROP TABLE IF EXISTS range_ratings_part" + str(table_sequence))
            cur.execute("CREATE TABLE range_ratings_part" + str(
                table_sequence) + " AS SELECT * FROM " + ratingstablename + " WHERE  Rating>=" + str(
                Range) + " AND Rating<=" + str(Range + Range_consider) + ";")
            table_sequence = table_sequence + 1
            Range = Range + Range_consider
        else:
            cur.execute("DROP TABLE IF EXISTS range_ratings_part" + str(table_sequence))
            cur.execute("CREATE TABLE range_ratings_part" + str(
                table_sequence) + " AS SELECT * FROM " + ratingstablename + " WHERE     Rating>" + str(
                Range) + " AND Rating<=" + str(Range + Range_consider) + ";")
            table_sequence = table_sequence + 1
            Range = Range + Range_consider

    cur.close()
    openconnection.commit()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    RROBIN_TABLE_PREFIX = 'round_robin_ratings_part'
    cur.execute("INSERT INTO META_DATA (key,value) VALUES ('NumPart'," + str(numberofpartitions) + ")")
    for i in range(0, numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cur.execute("create table " + table_name + " (userid integer, movieid integer, rating float);")
        cur.execute(
        "insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from (select userid, movieid, rating, ROW_NUMBER() over() as rnum from " + ratingstablename + ") as temp where mod(temp.rnum-1, 5) = " + str(
            i) + ";")
    cur.close()
    openconnection.commit()

def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    command = """INSERT INTO """ + ratingstablename + """ (userid,movieid,rating) VALUES (""" + str(
        userid) + """,""" + str(itemid) + """,""" + str(rating) + """);"""
    cur = openconnection.cursor()
    cur.execute(command)
    openconnection.commit()

    command = """SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'round_robin_ratings_part%';"""
    cur.execute(command)
    openconnection.commit()
    num_partitions = cur.fetchone()

    command = """SELECT * FROM (SELECT row_number() over(), * FROM """ + ratingstablename + """) AS temp WHERE userid = """ + str(
        userid) + """ AND movieid = """ + str(itemid) + """ AND rating = """ + str(rating) + """;"""
    cur.execute(command)
    openconnection.commit()
    row_id = cur.fetchone()

    part = (row_id[0] - 1) % (num_partitions[0])
    part_file = "round_robin_ratings_part" + str(part)
    command = """INSERT INTO """ + part_file + """ (userid,movieid,rating) VALUES (""" + str(userid) + """,""" + str(
        itemid) + """,""" + str(rating) + """);"""
    cur.execute(command)
    openconnection.commit()


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("select value from META_DATA where key='RangePart' ")
    RangePart = cur.fetchall()
    RangePart = int(RangePart[0][0])
    range2 = 5.0 / RangePart

    Lower_range = 0
    partitionnumber = 0
    Upper_rage = range2
    cur.execute("insert into " + ratingstablename + "(userid, movieid, rating) values (" + str(userid) + "," + str(
        itemid) + "," + str(rating) + ");")
    while Lower_range < 5.0:
        if Lower_range == 0:
            if rating >= Lower_range and rating <= Upper_rage:
                break
            partitionnumber = partitionnumber + 1
            Lower_range = Lower_range + range2
            Upper_rage = Upper_rage + range2
        else:
            if rating > Lower_range and rating <= Upper_rage:
                break
            partitionnumber = partitionnumber + 1
            Lower_range = Lower_range + range2
            Upper_rage = Upper_rage + range2

    print(partitionnumber)
    cur.execute(
        "INSERT INTO range_ratings_part" + str(partitionnumber) + " (UserID,MovieID,Rating) VALUES (%s, %s, %s)",
        (userid, itemid, rating))
    cur.close()
    openconnection.commit()


def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    cur = openconnection.cursor()
    cur.execute("select value from META_DATA where key='RangePart' ")
    RangePart = cur.fetchall()
    RangePart = int(RangePart[0][0])
    rangepart = []
    rangetable = "range_ratings_part"
    i = 0
    result = ''
    while (i < RangePart):
        cur.execute(
            "select * from  range_ratings_part" + str(i) + " where rating between " + str(ratingMinValue) + " and " + str(
                ratingMaxValue))
        a = []
        a.append(rangetable + str(i))
        a.append(cur.fetchall())
        listToStr = '\n'.join([rangetable + str(i) + ',' + str(elem)[1:-1] for elem in a[1]])
        result = result + '\n' + listToStr
        rangepart.append(a)
        i = i + 1

    table = 'round_robin_ratings_part'
    cur.execute("select value from META_DATA where key='NumPart' ")
    total_values = cur.fetchall()
    part = total_values[0][0]
    part = int(part)
    j = 0
    result1 = ''
    while (j < part):
        cur.execute(
            "select * from  " + table + str(j) + " where rating between " + str(ratingMinValue) + " and " + str(ratingMaxValue))
        a = []
        a.append(table + str(j))
        a.append(cur.fetchall())
        if not not a[1]:
            listToStr = '\n'.join([table + str(j) + ',' + str(elem)[1:-1] for elem in a[1]])
            result1 = result1 + '\n' + listToStr
        j = j + 1
    loadout = open(outputPath, 'w')
    loadout.write(result + result1)
    loadout.close()


def pointQuery(ratingValue, openconnection, outputPath):
    cur = openconnection.cursor()
    cur.execute("select value from META_DATA where key='RangePart' ")
    RangePart = cur.fetchall()
    RangePart = int(RangePart[0][0])
    slot = 5.0 / RangePart

    i = 0
    findpart = 0
    tableprefix = "range_ratings_part"
    rangepartdata = []
    while (not (i <= ratingValue and ratingValue <= i + slot)):
        i = i + slot
        findpart = findpart + 1

    tablename = "range_ratings_part" + str(findpart)
    cur.execute("select * from  " + tablename + " where rating= " + str(ratingValue))
    queryresult = cur.fetchall()
    listtostr1 = '\n'.join([tablename + "," + str(elem)[1:-1] for elem in queryresult])
    j = 0
    roundrobinlist = []
    table = 'round_robin_ratings_part'
    cur.execute("select value from META_DATA where key='NumPart' ")
    total_values = cur.fetchall()
    part = total_values[0][0]
    part = int(part)
    b = []

    while (j < part):
        cur.execute("select * from  " + table + str(j) + " where rating= " + str(ratingValue))
        a = []
        tableName = table + str(j)
        a.append(table + str(j))
        a.append(cur.fetchall())
        if not not a[1]:
            b.append(a)

        j = j + 1
    loadout = open(outputPath, 'w')
    listToStr = '\n'.join([str(elem[0]) + ',' + str(elem[1])[2:-2] for elem in b])
    loadout.write(listtostr1 + '\n' + listToStr)
    loadout.close()


def createDB(dbname='dds_assignment1'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()



