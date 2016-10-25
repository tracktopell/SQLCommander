package com.tracktopell.dbutil.sqlcommander;

import java.io.*;

import java.sql.*;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * com.tracktopell.dbutil.sqlcommander.Main
 * @author Alfredo Estrada
 */
public class Main {

    protected static final String PARAM_CONNECTION_JDBC_CLASS_DRIVER = "jdbc.driverClassName";
    protected static final String PARAM_CONNECTION_JDBC_URL = "jdbc.url";
    protected static final String PARAM_CONNECTION_JDBC_USER = "jdbc.user";
    protected static final String PARAM_CONNECTION_JDBC_PASSWORD = "jdbc.password";

    protected Properties connectionProperties;
    protected boolean printInfoDBOnStartup = false;

    protected static Logger logger = Logger.getLogger(Main.class.getSimpleName());

    public Main(Properties p) throws IOException {
        logger.finer("init(): try to load properties :" + p);
        this.connectionProperties = p;
    }

    public void setPrintInfoDBOnStartup(boolean printInfoDBOnStartup) {
        this.printInfoDBOnStartup = printInfoDBOnStartup;
    }

    public boolean isPrintInfoDBOnStartup() {
        return printInfoDBOnStartup;
    }
    
    protected Connection getConnection() throws IllegalStateException, SQLException {
        Connection conn = null;
        try {
            logger.finer("getConnection: ...try get Connection (using " + connectionProperties + ")for Create DB.");
            Class.forName(connectionProperties.getProperty(PARAM_CONNECTION_JDBC_CLASS_DRIVER)).newInstance();
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException(ex.getMessage());
        } catch (InstantiationException ex) {
            throw new IllegalStateException(ex.getMessage());
        } catch (IllegalAccessException ex) {
            throw new IllegalStateException(ex.getMessage());
        }

        logger.finer("getConnection:Ok, Loaded JDBC Driver.");
        String urlConnection = connectionProperties.getProperty("jdbc.url");

        if (urlConnection.contains("${db.name}")) {
            urlConnection = urlConnection.replace("${db.name}", connectionProperties.getProperty("db.name"));
            logger.finer("getConnection:replacement for variable db.name, now urlConnection=" + urlConnection);
        }

        conn = DriverManager.getConnection(
                urlConnection,
                connectionProperties.getProperty(PARAM_CONNECTION_JDBC_USER),
                connectionProperties.getProperty(PARAM_CONNECTION_JDBC_PASSWORD));

        logger.finer("getConnection:OK Connected to DB.");
        if(printInfoDBOnStartup){
            printDBInfo(conn);
        }
        return conn;
    }

    private void printDBInfo(Connection conn) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();

        System.out.println("\t=>>SchemaTerm:" + metaData.getSchemaTerm());

        System.out.println("Schemas:");

        ResultSet schemas = metaData.getSchemas();
        while (schemas.next()) {
            System.out.println("\t=>>" + schemas.getString("TABLE_SCHEM") + ", " + schemas.getString("TABLE_CATALOG"));
        }
        schemas.close();
        ResultSet tablesRS = metaData.getTables(null, null, "%", null);
        System.out.println("Tables:");
        Statement statement = conn.createStatement();
        while (tablesRS.next()) {
            String schemaTableIter = tablesRS.getString(2);
            String tableNameIter = tablesRS.getString(3);
            if (schemaTableIter.toLowerCase().contains("sys")) {
                continue;
            }
            System.out.print("\t" + schemaTableIter + "." + tableNameIter + "(");
            ResultSet executeQuery = statement.executeQuery("SELECT * FROM " + schemaTableIter + "." + tableNameIter + " WHERE 1=2");
            ResultSetMetaData emptyTableMetaData = executeQuery.getMetaData();
            int columnCount = emptyTableMetaData.getColumnCount();
            for (int columNumber = 1; columNumber <= columnCount; columNumber++) {
                if (columNumber > 1) {
                    System.out.println(",");
                } else {
                    System.out.println("");
                }
                int columnSize = emptyTableMetaData.getPrecision(columNumber);
                int columnDD = emptyTableMetaData.getScale(columNumber);
                int nullableFlag = emptyTableMetaData.isNullable(columNumber);
                boolean autoIncrementFlag = emptyTableMetaData.isAutoIncrement(columNumber);

                System.out.print("\t\t" + emptyTableMetaData.getColumnName(columNumber) + "  " + emptyTableMetaData.getColumnTypeName(columNumber));
                if (columnSize > 0) {
                    System.out.print(" ( " + columnSize);
                    if (columnDD > 0) {

                    }
                    System.out.print(" )");
                }
                if (nullableFlag == 1) {
                    System.out.print(" NULL");
                }
                if (autoIncrementFlag) {
                    System.out.print(" AUTOINCREMENT");
                }
            }
            System.out.println();
            System.out.println("\t);");
            executeQuery.close();

            /*
			ResultSet resColumnsTable = metaData.getColumns(null, schemaTableIter, tableNameIter, null);
			for(int columnCounter = 0;resColumnsTable.next();columnCounter++) {
				if(columnCounter>0){
					System.out.println(",");
				}else{
					System.out.println("");
				}
				
				
				System.out.print("\t\t" + 
						resColumnsTable.getString("COLUMN_NAME")+ "  " + resColumnsTable.getString("TYPE_NAME"));
				
				int columnSize = resColumnsTable.getInt("COLUMN_SIZE");
				int columnDD   = resColumnsTable.getInt("DECIMAL_DIGITS");
				int nullableFlag = resColumnsTable.getInt("NULLABLE");
				boolean autoIncrementFlag = resColumnsTable.getString("IS_AUTOINCREMENT").equalsIgnoreCase("yes");
				
				if(columnSize>0) {
					System.out.print(" ( " + columnSize);
					if(columnDD>0){
					
					}
					System.out.print(" )");
				}
				if(nullableFlag ==1) {
					System.out.print(" NULL");
				}
				if(autoIncrementFlag) {
					System.out.print(" AUTOINCREMENT");
				}				
			}
			System.out.println();
			System.out.println("\t);");
			resColumnsTable.close();
             */
        }
        tablesRS.close();

        System.out.println("=======================================");
    }

    protected void executeScriptFrom(InputStream is, Connection conn, boolean continueWithErrors)
            throws SQLException, IOException {

        BufferedReader brInput = null;
        ResultSet rs = null;
        ResultSetMetaData rsmd = null;
        String sql = null;

        int updateCount;
        int numberOfColumns;
        boolean prinToConsole = true;

        prinToConsole = true;

        try {
            conn.setAutoCommit(true);
            brInput = new BufferedReader(new InputStreamReader(is));

            if (prinToConsole) {
                System.out.print("sql > ");
            }
            Statement sexec = conn.createStatement();

            String fullSql = "";
            while ((sql = brInput.readLine()) != null) {
                if (is != System.in) {
                    if (prinToConsole) {
                        System.out.println(sql);
                    }
                }
                if (sql.trim().toLowerCase().equals("exit")) {
                    break;
                } else if (sql.trim().toLowerCase().equals("!dbinfo")) {
                    printDBInfo(conn);
                    if (prinToConsole) {
                        System.out.println("");
                        System.out.print("sql > ");
                    }
                    continue;
                }
                if (sql.trim().length() == 0 || sql.startsWith("--")) {
                    if (prinToConsole) {
                        System.out.print("sql > ");
                    }
                    continue;
                } else if (sql.trim().endsWith(";")) {
                    fullSql += " " + sql.trim();
                    try {
                        fullSql = fullSql.replaceAll(";$", "");
                        boolean resultExecution = false;
                        if (fullSql.toLowerCase().startsWith("call ")) {
                            //fullSql = fullSql.replace("call ","");
                            //System.err.println("==>>conn.prepareCall("+fullSql+")");
                            CallableStatement callSt = conn.prepareCall(fullSql);

                            resultExecution = callSt.execute();
                            rs = resultExecution ? callSt.getResultSet() : null;
                        } else {
                            resultExecution = sexec.execute(fullSql);
                            rs = resultExecution ? sexec.getResultSet() : null;
                        }

                        if (resultExecution && rs != null) {

                            rsmd = rs.getMetaData();
                            numberOfColumns = rsmd.getColumnCount();
                            if (prinToConsole) {
                                System.out.print("Resultset{\n");
                            }
                            if (prinToConsole) {
                                System.out.print("\tClassResultSet{\n\t\t");
                            }
                            for (int j = 0; j < numberOfColumns; j++) {
                                if (prinToConsole) {
                                    System.out.print((j > 0 ? ",'" : "'") + rsmd.getColumnClassName(j + 1) + "'");
                                }
                            }
                            if (prinToConsole) {
                                System.out.print("\n\t},\n");
                            }

                            if (prinToConsole) {
                                System.out.print("\tHeaderLabels{\n\t\t");
                            }
                            for (int j = 0; j < numberOfColumns; j++) {
                                if (prinToConsole) {
                                    System.out.print((j > 0 ? ",'" : "'") + rsmd.getColumnLabel(j + 1) + "'");
                                }
                            }
                            if (prinToConsole) {
                                System.out.print("\n\t},\n");
                            }

                            if (prinToConsole) {
                                System.out.print("\tDataRows{");
                            }
                            int numRows;
                            for (numRows = 0; rs.next(); numRows++) {
                                if (numRows > 0) {
                                    if (prinToConsole) {
                                        System.out.print(",");
                                    }
                                }
                                if (prinToConsole) {
                                    System.out.print("\n\t\t{");
                                }
                                for (int j = 0; j < numberOfColumns; j++) {

                                    if (prinToConsole) {
                                        Object o = rs.getObject(j + 1);
                                        if (o == null) {
                                            System.out.print((j > 0 ? ", <NULL>" : "<NULL>"));
                                        } else if (o.getClass().equals(String.class)) {
                                            System.out.print((j > 0 ? "," : "") + "'" + rs.getString(j + 1) + "'");
                                        } else {
                                            System.out.print((j > 0 ? "," : "") + rs.getString(j + 1));
                                        }
                                    }
                                }
                                if (prinToConsole) {
                                    System.out.print(" }");
                                }
                            }
                            rs.close();
                            if (prinToConsole) {
                                System.out.print("\n\t}.size()=" + numRows + "\n");
                            }
                            if (prinToConsole) {
                                System.out.print("};\n");
                            }
                        } else {
                            updateCount = sexec.getUpdateCount();
                            if (prinToConsole) {
                                System.out.print(updateCount + " rows affected\n");
                            }
                        }
                    } catch (Exception exExec) {
                        if (prinToConsole) {
                            exExec.printStackTrace(System.err);
                            //System.err.print("\t[x]:" + exExec.getMessage() + "\n");
                        }
                        if (!continueWithErrors) {
                            break;
                        }
                    }
                    fullSql = "";
                    if (prinToConsole) {
                        System.out.print("sql > ");
                    }
                } else {
                    fullSql += " " + sql.trim();
                    if (prinToConsole) {
                        System.out.print("    > ");
                    }
                }
            }
            if (prinToConsole) {
                System.out.println("Script executed.");
            }
        } catch (SQLException ex) {
            throw ex;
        } catch (IOException ex2) {
            throw ex2;
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (Exception ex3) {
                ex3.printStackTrace(System.err);
            }
        }
    }

    public void shellDB() {

        logger.finer("shellDB: --------------");

        Connection conn = null;

        try {
            conn = getConnection();
            logger.finer("shellDB:OK, the DB exist !!");
            logger.finer("shellDB:Ready, Now read from stdin, connectionForInit=" + conn);
            executeScriptFrom(System.in, conn, true);
            logger.finer("-> EOF stdin, end");
        } catch (IOException ex) {
            logger.log(Level.SEVERE, "Something with the reading script:" + ex.getLocalizedMessage(), ex);
        } catch (IllegalStateException ex) {
            logger.log(Level.SEVERE, "Something with the Classpath and JDBC Driver:" + ex.getLocalizedMessage(), ex);
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Something with the JDBC Connection:" + ex.getLocalizedMessage(), ex);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex1) {
                logger.log(Level.SEVERE, ex1.getLocalizedMessage(), ex1);
            }
        }
    }

    private void extractResource(String resourceToExtract, String resourcePathToExtract) throws FileNotFoundException, IOException {
        InputStream is = getClass().getResourceAsStream(resourcePathToExtract);
        OutputStream os = new FileOutputStream(resourceToExtract);
        int r;
        byte[] buffer = new byte[1024 * 128];
        while ((r = is.read(buffer, 0, buffer.length)) != -1) {
            os.write(buffer, 0, r);
        }
        is.close();
        os.close();
    }

    public static void main(String args[]) {
        Main dbInstaller;
        String driver   = null;
        String url      = null;
        String user     = null;        
        String password = null;
        
        boolean printInfoDBOnStartup = false;
        
        String prevArg  = null;                
        String argValue = null;
        
        for(String arg: args){                            
            if(prevArg != null){
                argValue = arg;
                if(prevArg.equals("-driverClass")){
                    driver   = argValue;
                    logger.fine("  ->driver="+driver);
                } else if(prevArg.equals("-url")){
                    url      = argValue;
                    logger.fine("     ->url="+url);
                } else if(prevArg.equals("-user")){
                    user     = argValue;
                    logger.fine("    ->user="+user);
                } else if(prevArg.equals("-password")){
                    password = argValue;
                    logger.fine("->password="+password);
                } else if(prevArg.equalsIgnoreCase("-printDBInfoOnStatup")){
                    printInfoDBOnStartup = true;
                    logger.fine("->printInfoDBOnStartup="+printInfoDBOnStartup);
                }
            }
            prevArg =  arg;
        }

        if(driver == null){
            printUssage();
            System.exit(1);
        }
        if(url == null){
            printUssage();
            System.exit(1);
        }
        if(user == null){
            printUssage();
            System.exit(1);
        }
        if(password == null){
            printUssage();
            System.exit(1);
        }
        
        Properties parameters4CreateAndExecute=new Properties();
        
        parameters4CreateAndExecute.put(PARAM_CONNECTION_JDBC_CLASS_DRIVER, driver);
        parameters4CreateAndExecute.put(PARAM_CONNECTION_JDBC_URL         , url);
        parameters4CreateAndExecute.put(PARAM_CONNECTION_JDBC_USER        , user);
        parameters4CreateAndExecute.put(PARAM_CONNECTION_JDBC_PASSWORD    , password);
        
        try {
            dbInstaller = new Main(parameters4CreateAndExecute);
            dbInstaller.setPrintInfoDBOnStartup(printInfoDBOnStartup);            
            logger.fine("----------------------------- SQLCommanderPrompt -----------------------------");
            dbInstaller.shellDB();
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
        }
    }

    private static void printUssage() {
        System.err.println("\t----------------------------- SQLCommanderPrompt -----------------------------");        
        System.err.println("usage:\t");
        System.err.println("\tcom.tracktopell.dbutil.sqlcommander.SQLCommanderPrompt -driverClass com.db.driver.ETC  -url \"jdbc:db://127.0.0.1:80/db\" -user xxxx -password yyy");
    }    
}
