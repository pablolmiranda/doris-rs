suite("suite") {
    sql new File("""${context.file.parent}/ddl/t.sql""").text
    qt_select "select * from t"
}
