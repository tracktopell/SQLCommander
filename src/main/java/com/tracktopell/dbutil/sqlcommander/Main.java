package com.tracktopell.dbutil.sqlcommander;

import java.io.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * com.tracktopell.dbutil.sqlcommander.Main
 * @author Alfredo Estrada
 */
public class Main {
	private static       Properties versionProperties;
	public  static final String VERSION_LOCATION = "/com/tracktopell/sqlcommander/version.properties";
	public  static final String BUILT_TIMESTAMP = "version.build.timestamp";
	public  static final String PROJECT_VERSION = "project.version";
	
    public static final String PARAM_CONNECTION_JDBC_CLASS_DRIVER = "jdbc.driverClassName";
    public static final String PARAM_CONNECTION_JDBC_URL = "jdbc.url";
    public static final String PARAM_CONNECTION_JDBC_USER = "jdbc.user";
    public static final String PARAM_CONNECTION_JDBC_PASSWORD = "jdbc.password";
    public static final String PARAM_CONNECTION_JDBC_CATALOG  = "jdbc.catalog";
    public static final String PARAM_CONNECTION_JDBC_SCHEMMA  = "jdbc.schemma";

    protected Properties connectionProperties;
    protected boolean printInfoDBOnStartup = false;	
	protected static String rdbms = "[SQL]";

    protected static Logger logger = Logger.getLogger(Main.class.getSimpleName());

    public Main(Properties p) throws IOException {
        logger.fine("init(): try to load properties :" + p);
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
            logger.fine("getConnection: ...try get Connection (using " + connectionProperties + ")for Create DB.");
            Class.forName(connectionProperties.getProperty(PARAM_CONNECTION_JDBC_CLASS_DRIVER)).newInstance();
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException(ex.getMessage());
        } catch (InstantiationException ex) {
            throw new IllegalStateException(ex.getMessage());
        } catch (IllegalAccessException ex) {
            throw new IllegalStateException(ex.getMessage());
        }

        logger.fine("getConnection:Ok, Loaded JDBC Driver.");
        String urlConnection = connectionProperties.getProperty("jdbc.url");

        if (urlConnection.contains("${db.name}")) {
            urlConnection = urlConnection.replace("${db.name}", connectionProperties.getProperty("db.name"));
            logger.fine("getConnection:replacement for variable db.name, now urlConnection=" + urlConnection);
        }

        conn = DriverManager.getConnection(
                urlConnection,
                connectionProperties.getProperty(PARAM_CONNECTION_JDBC_USER),
                connectionProperties.getProperty(PARAM_CONNECTION_JDBC_PASSWORD));

        logger.fine("getConnection:OK Connected to DB.");
        if(printInfoDBOnStartup){
            printDBInfo(conn,connectionProperties.getProperty(PARAM_CONNECTION_JDBC_CATALOG),connectionProperties.getProperty(PARAM_CONNECTION_JDBC_SCHEMMA));
        }
        return conn;
    }

    private void printDBInfo(Connection conn,String catalog,String schemma) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();

        System.out.println("\t=>>SchemaTerm:" + metaData.getSchemaTerm());
        ResultSet schemsRS = metaData.getSchemas();
        while (schemsRS.next()) {
            System.out.println("=>Schemas: TABLE_SCHEM="+schemsRS.getString("TABLE_SCHEM")+", TABLE_CATALOG="+schemsRS.getString("TABLE_CATALOG"));
        }
        schemsRS.close();
        
        ResultSet catsRS = metaData.getCatalogs();
        while (catsRS.next()) {
            System.out.println("=>Catalogs: TABLE_CAT="+catsRS.getString("TABLE_CAT"));
        }
        catsRS.close();
        ResultSet tablesRS = metaData.getTables(catalog, schemma, "%", null);
        System.out.println("Tables:");
		List<String> tableNames=new ArrayList<>();
		while (tablesRS.next()) {			
			tableNames.add(tablesRS.getString(3));
			System.out.println("\tTABLE ["+tablesRS.getString(2)+"."+tablesRS.getString(3)+"]");
		}

        Statement statement = conn.createStatement();
		
        for(String tableNameIter: tableNames){
			            
			System.out.print("\t" + tableNameIter + "(");
			
			ResultSet rsFKs = metaData.getImportedKeys(catalog, schemma, tableNameIter);
			HashMap<String,String> tableFKs= new HashMap<>();
			while(rsFKs.next()){
				tableFKs.put(rsFKs.getString("FKCOLUMN_NAME"), rsFKs.getString("PKTABLE_NAME")+"."+rsFKs.getString("PKCOLUMN_NAME"));
			}
			rsFKs.close();
			
			ResultSet rsPKs = metaData.getPrimaryKeys(catalog, schemma, tableNameIter);
			HashSet<String> tablePKs= new HashSet<>();
			while(rsPKs.next()){
				tablePKs.add(rsPKs.getString("COLUMN_NAME"));
			}
			rsPKs.close();
			
        	ResultSet resColumnsTable = metaData.getColumns(catalog, schemma, tableNameIter, null);
			
			for(int columnCounter = 0;resColumnsTable.next();columnCounter++) {
				if(columnCounter>0){
					System.out.println(",");
				}else{
					System.out.println("");
				}
				
				int columnSize = resColumnsTable.getInt("COLUMN_SIZE");
				int columnDD   = resColumnsTable.getInt("DECIMAL_DIGITS");
				int nullableFlag = resColumnsTable.getInt("NULLABLE");
				boolean autoIncrementFlag = resColumnsTable.getString("IS_AUTOINCREMENT").equalsIgnoreCase("yes");
				boolean isPK              = tablePKs.contains(resColumnsTable.getString("COLUMN_NAME"));
				
				if(isPK) {
					System.out.print("\t\t->");
				} else{
					System.out.print("\t\t  ");
				}
				
				System.out.print(resColumnsTable.getString("COLUMN_NAME")+ "  " + resColumnsTable.getString("TYPE_NAME"));
				
				if(columnSize>0) {
					System.out.print(" ( " + columnSize);
					if(columnDD>0){
					
					}
					System.out.print(" )");
				}
				if(nullableFlag ==1) {
					System.out.print(" NULL");
				}
				if(isPK) {
					System.out.print(" PRIMARY KEY");
				}
				if(autoIncrementFlag) {
					System.out.print(" AUTOINCREMENT");
				}
				
				
				String stringFKs = tableFKs.get(resColumnsTable.getString("COLUMN_NAME"));
				if(stringFKs != null){
					System.out.print(" => "+stringFKs);
				}
			}
			System.out.println();
			System.out.println("\t);");
			resColumnsTable.close();
            
        }
		
        tablesRS.close();
		System.out.println("\tTABLES COUNT:"+tableNames.size());
        System.out.println("\t=======================================");
    }

    public int executeScriptFrom(InputStream is,String rdbmsPrompt, Connection conn, boolean prinToConsole, boolean repeatInput)
            throws SQLException, IOException {

        BufferedReader brInput = null;
        ResultSet rs = null;
        ResultSetMetaData rsmd = null;
        String sqlInput = null;
		String promt1   = rdbmsPrompt+"> ";
		String promt2   = "      >>";
        int updateCount;
        int numberOfColumns;
        
        prinToConsole = true;
		int errCounter=0;
        try {
            conn.setAutoCommit(true);
            brInput = new BufferedReader(new InputStreamReader(is));

            if (prinToConsole) {
                System.out.print(promt1);
            }
            Statement sexec = conn.createStatement();

            String fullSql = "";
            while ((sqlInput = brInput.readLine()) != null) {
                if (repeatInput) {
                    if (prinToConsole) {
                        System.out.println(sqlInput);
                    }
                }
                if (sqlInput.trim().toLowerCase().equalsIgnoreCase("exit")) {
                    break;
                } else if (sqlInput.trim().toLowerCase().equalsIgnoreCase("!dbinfo")) {
                    printDBInfo(conn,connectionProperties.getProperty(PARAM_CONNECTION_JDBC_CATALOG),connectionProperties.getProperty(PARAM_CONNECTION_JDBC_SCHEMMA));
                    if (prinToConsole) {
                        System.out.println("");
                        System.out.print(promt1);
                    }
                    continue;
                }
                if (sqlInput.trim().length() == 0 || sqlInput.startsWith("--")) {
                    if (prinToConsole) {
                        System.out.print(promt1);
                    }
                    continue;
                } else if (sqlInput.trim().endsWith(";")) {
                    fullSql += " " + sqlInput.trim();
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
								System.out.print("\n--------------\n");
                            }
                            
                            for (int j = 0; j < numberOfColumns; j++) {
                                if (prinToConsole) {
                                    System.out.print((j > 0 ? "|'" : "'") + rsmd.getColumnClassName(j + 1) + "'");
                                }
                            }
                            if (prinToConsole) {           
								System.out.print("\n--------------\n");
                            }
                            for (int j = 0; j < numberOfColumns; j++) {
                                if (prinToConsole) {
                                    System.out.print((j > 0 ? "|'" : "'") + rsmd.getColumnLabel(j + 1) + "'");
                                }
                            }
                            if (prinToConsole) {                                
								System.out.print("\n--------------\n");
                            }
                            int numRows;
                            for (numRows = 0; rs.next(); numRows++) {                                
                                if (prinToConsole) {
                                    //System.out.print("\n\t\t{");
                                }
                                for (int j = 0; j < numberOfColumns; j++) {

                                    if (prinToConsole) {
										
                                        Object o = rs.getObject(j + 1);
                                        if (o == null) {
                                            System.out.print((j > 0 ? "|NULL" : "NULL"));
                                        } else if (o.getClass().equals(String.class)) {
                                            System.out.print((j > 0 ? "|" : "") + "'" + rs.getString(j + 1) + "'");
                                        } else {
                                            System.out.print((j > 0 ? "|" : "") + rs.getString(j + 1));
                                        }
                                    }
                                }
                                if (prinToConsole) {
									System.out.print("\n");
                                    //System.out.print(" }");
                                }
                            }
                            rs.close();
                            if (prinToConsole) {                                
								System.out.print("--------------\n");
								System.out.print(numRows+" rows.\n");
                            }
                            if (prinToConsole) {
                                //System.out.print("};\n");
                            }
                        } else {
                            updateCount = sexec.getUpdateCount();
                            if (prinToConsole) {
								System.out.print("--------------\n");
                                System.out.print(updateCount + " rows affected.\n");
                            }
                        }
                    } catch (Exception exExec) {
                        if (prinToConsole) {
                            //exExec.printStackTrace(System.err);
                            System.err.print("\t[x]:" + exExec.getMessage() + "\n");
                        }
                        errCounter++;
                        if (!continueWithErrors) {
							System.err.println("\t[^] not continue With Errors("+continueWithErrors+"), break");                            
                            break;
                        }
                    }
                    fullSql = "";
                    if (prinToConsole) {
                        System.out.print(promt1);
                    }
                } else {
                    fullSql += " " + sqlInput.trim();
                    if (prinToConsole) {
                        System.out.print(promt2);
                    }
                }
            }
            if (prinToConsole) {
				System.out.println("{EOF}");
				if(errCounter>0){
					System.err.println("\t[X] Error Counter:"+errCounter);
				}
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
            return errCounter;
        }
    }

	public int shellDB() {
		return shellDB(System.in);
	}
	
    public int shellDB(InputStream is) {
		boolean prinToConsole = true;
		boolean repeatInput   = false;
		Console console = null;
		int exitStatus = -1;
		try{
			console = System.console();
			logger.fine("shellDB: console = "+console);
			if(console == null){
				repeatInput   = true;
			}
		}catch(Exception e){
			logger.log(Level.SEVERE, "->shellDB:", e);
		}
        logger.fine("shellDB: --------------");

        Connection conn = null;
        int errCounter=0;
        try {
            conn = getConnection();
            logger.fine("shellDB:OK, the DB exist !!");
            logger.fine("shellDB:Ready, Now read from stdin(is pipe?"+repeatInput+"), connectionForInit=" + conn);
            errCounter = executeScriptFrom(is, rdbms, conn, prinToConsole, repeatInput );
            logger.fine("-> executeScriptFrom: errCounter="+errCounter);
            if(errCounter == 0){
                exitStatus = 0;
            } else{
                exitStatus = 4;
            }
            logger.fine("-> EOF stdin, end");
			
        } catch (IOException ex) {
            logger.log(Level.SEVERE, "Something with the reading script:" + ex.getLocalizedMessage(), ex);
			exitStatus = 5;
        } catch (IllegalStateException ex) {
            logger.log(Level.SEVERE, "Something with the Classpath and JDBC Driver:" + ex.getLocalizedMessage(), ex);
			exitStatus = 6;
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Something with the JDBC Connection:" + ex.getLocalizedMessage(), ex);
			exitStatus = 7;
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex1) {
                logger.log(Level.SEVERE, ex1.getLocalizedMessage(), ex1);
				exitStatus = 8;
            }
        }
		return exitStatus;
    }

	private static boolean continueWithErrors=false;
	
    public static void main(String args[]) {
        Main sqlCommanderMain;
        String driver   = null;
        String url      = null;
        String user     = null;        
        String password = null;
        String catalog  = null;
        String schemma  = null;

        
        boolean printInfoDBOnStartup = false;        
		String prevArgName =null;
		String argName =null;
		String argValue=null;
			
        for(String arg: args){
			logger.fine("-> arg["+arg+"]");			
			if( argName == null){
				argName = arg;
			} else {
				if(argValue == null){
					argValue = arg;

					logger.fine("\t["+argName+"] = ["+argValue+"]");
					
					if(argName.equals("-driverClass")){
						driver   = argValue;
						logger.fine("\t\t==>> driver=["+driver+"]");
					} else if(argName.equals("-url")){
						url      = argValue;
						logger.fine("url=["+url+"]");
					} else if(argName.equals("-user")){
						user     = argValue;
						logger.fine("\t\t==> user=["+user+"]");
					} else if(argName.equals("-password")){
						password = argValue;
						logger.fine("\t\t==> password=["+password+"]");
					} else if(argName.equalsIgnoreCase("-printDBInfoOnStatup") && argValue.equals("true")){
						printInfoDBOnStartup = true;
						logger.fine("\t\t==> printInfoDBOnStartup="+printInfoDBOnStartup);
					} else if(argName.equalsIgnoreCase("-continueWithErrors") && argValue.equals("true")){
						continueWithErrors=true;
						logger.fine("\t\t==> continueWithErrors=true");
					} else if(argName.equals("-catalog")){
						catalog      = argValue;
						logger.fine("catalog=["+catalog+"]");
					} else if(argName.equals("-schemma")){
						schemma      = argValue;
						logger.fine("schemma=["+schemma+"]");
					}
					argName  = null;
					argValue = null;		
					logger.fine("\t<<------------");
				}
			}
			
			prevArgName = arg;
        }
		
		printSplash();
		
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
        if(catalog!=null && !catalog.equalsIgnoreCase("null")){
            parameters4CreateAndExecute.put(PARAM_CONNECTION_JDBC_CATALOG , catalog);
        }
        if(schemma!=null && !schemma.equalsIgnoreCase("null")){
            parameters4CreateAndExecute.put(PARAM_CONNECTION_JDBC_SCHEMMA , schemma);
        }
        int exitStatus = 0;
        try {
			if(driver.contains("mysql")){
				rdbms = " MySQL";
			} else if(driver.contains("derby")){
				rdbms = " Derby";
			} else if(driver.contains("oracle")){
				rdbms = "Oracle";
			} else if(driver.contains("microsoft")){
				rdbms = " MsSQL";
			} else {
				rdbms = "   SQL";
			}
			
            sqlCommanderMain = new Main(parameters4CreateAndExecute);
            sqlCommanderMain.setPrintInfoDBOnStartup(printInfoDBOnStartup);            
            logger.fine("----------------------------- SQLCommanderPrompt -----------------------------");
            exitStatus = sqlCommanderMain.shellDB();
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
        }
        //System.err.println(" <<< "+exitStatus);
		System.exit(exitStatus);
    }

	private static void printSplash() {
		Properties vp=loadVersionProperties();
        System.err.println("\t----------------------------- Tracktopell : SQLCommander -----------------------------");      
		System.err.println("\tBUILD  : \t"+vp.getProperty(BUILT_TIMESTAMP));
		System.err.println("\tVERSION: \t"+vp.getProperty(PROJECT_VERSION));        
    }
    private static void printUssage() {		
        System.err.println("usage:\t");
        System.err.println("\tcom.tracktopell.dbutil.sqlcommander.Main -driverClass com.db.driver.ETC  -ur \"jdbc:db://127.0.0.1:80/db\" -user xxxx -password yyy [ -printDBInfoOnStatup [true|false] ] [-continueWithErrors [true|false]] [-catalog=CATALOG] [-schemma=SCHEMMA]");
    }
	
	private static Properties loadVersionProperties(){
		if(versionProperties == null){
			versionProperties = new Properties();
			try {
				versionProperties.load(Main.class.getResourceAsStream(VERSION_LOCATION));				
			}catch(IOException ioe){
				logger.log(Level.WARNING, "loadVersionProperties, cant find ", ioe);				
				versionProperties.put(BUILT_TIMESTAMP, "?");				
				versionProperties.put(PROJECT_VERSION, "?");
			}
		}
		return versionProperties;
	}
}
