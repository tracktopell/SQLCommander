package com.tracktopell.dbutil.sqlcommander;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;

import java.sql.*;
import java.text.SimpleDateFormat;
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

	public static final String DEFAULT_FIELD_SEPARATOR    = ",";
	public static final String DEFAULT_STRING_DELIMITATOR = "'";
	
    protected Properties connectionProperties;
    protected static boolean printInfoDBOnStartup = false;
	protected static boolean printMetadata		  = false;
    protected static boolean printHeaders		  = true;	
	protected static boolean quietExport          = false;
	protected static String fieldSeprator      = DEFAULT_FIELD_SEPARATOR;
	protected static String stringDelimitator  = DEFAULT_STRING_DELIMITATOR;
	protected static final SimpleDateFormat df_datetime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	protected static final SimpleDateFormat df_date     = new SimpleDateFormat("yyyy-MM-dd");

	protected static String rdbms = "[SQL]";
    protected static Logger logger = Logger.getLogger(Main.class.getSimpleName());
	private static boolean continueWithErrors=false;

	public Main(Properties p) throws IOException {
        logger.info("init(): try to load properties :" + p);
        this.connectionProperties = p;
    }

    public static void main(String args[]) {
        Main sqlCommanderMain;
        String driver   = null;
        String url      = null;
        String user     = null;        
        String password = null;
        String catalog  = null;
        String schemma  = null;
        	
		String prevArgName =null;
		String argName =null;
		String argValue=null;
		
		logger.setLevel(Level.WARNING);
		logger.info("----------------[Main]------------");
        for(String arg: args){
			logger.info("-> arg["+arg+"]");
			if( argName == null){
				argName = arg;
			} else {
				if(argValue == null){
					argValue = arg;

					logger.info("\t["+argName+"] = ["+argValue+"]");
					
					if(argName.equals("-driverClass")){
						driver   = argValue;
						logger.info("\t\t==>> driver=["+driver+"]");
					} else if(argName.equals("-url")){
						url      = argValue;
						logger.info("url=["+url+"]");
					} else if(argName.equals("-user")){
						user     = argValue;
						logger.info("\t\t==> user=["+user+"]");
					} else if(argName.equals("-password")){
						password = argValue;
						logger.info("\t\t==> password=["+password+"]");
					} else if(argName.equalsIgnoreCase("-printDBInfoOnStatup") && argValue.equals("true")){
						printInfoDBOnStartup = true;
						logger.info("\t\t==> printInfoDBOnStartup="+printInfoDBOnStartup);
					} else if(argName.equalsIgnoreCase("-continueWithErrors") && argValue.equals("true")){
						continueWithErrors=true;
						logger.info("\t\t==> continueWithErrors=true");
					} else if(argName.equals("-catalog")){
						catalog      = argValue;
						logger.info("catalog=["+catalog+"]");
					} else if(argName.equals("-schemma")){
						schemma      = argValue;
						logger.info("schemma=["+schemma+"]");
					} else if(argName.equals("-fs")){
						fieldSeprator= argValue;
						if(fieldSeprator.contains("\\t")){
							fieldSeprator = fieldSeprator.replace("\\t","\t");
						}
						logger.info("fieldSeprator=["+fieldSeprator+"]");
					} else if(argName.equals("-sd")){
						stringDelimitator= argValue;
						logger.info("fieldSeprator=["+fieldSeprator+"]");
					} else if(argName.equalsIgnoreCase("-q") && (argValue.equalsIgnoreCase("true")||argValue.equalsIgnoreCase("false"))){
						quietExport = Boolean.parseBoolean(argValue);
						logger.info("quietExport="+quietExport);
					} else if(argName.equalsIgnoreCase("-H") && (argValue.equalsIgnoreCase("true")||argValue.equalsIgnoreCase("false"))){
						printHeaders = Boolean.parseBoolean(argValue);
						logger.info("printHeaders="+printHeaders);
					} else if(argName.equalsIgnoreCase("-M") && (argValue.equalsIgnoreCase("true")||argValue.equalsIgnoreCase("false"))){
						printMetadata = Boolean.parseBoolean(argValue);
						logger.info("printMetadata="+printMetadata);
					} else if(argName.equals("-l")){
						Level myLevel = null;
						myLevel = 
								argValue.equalsIgnoreCase("FINEST")?	Level.FINEST:
								argValue.equalsIgnoreCase("FINER")?		Level.FINER:
								argValue.equalsIgnoreCase("FINE")?		Level.FINE:
								argValue.equalsIgnoreCase("INFO")?		Level.INFO:
								argValue.equalsIgnoreCase("SEVERE")?	Level.SEVERE:
								argValue.equalsIgnoreCase("WARNING")?	Level.WARNING:
								argValue.equalsIgnoreCase("CONFIG")?	Level.CONFIG:
								argValue.equalsIgnoreCase("OFF")?		Level.OFF:
																		Level.WARNING;
						logger.setLevel(myLevel);
					} 
					
					argName  = null;
					argValue = null;		
					logger.info("\t<<------------");
				}
			}
			
			prevArgName = arg;
        }
		
		if(!quietExport){
			printSplash();
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
			} else if(driver.contains("postgresql")){
				rdbms = "PG-SQL";
			} else if(driver.contains("as400")){
				rdbms = " AS400";
			} else {
				rdbms = "   SQL";
			}
			
            sqlCommanderMain = new Main(parameters4CreateAndExecute);
            logger.info("----------------------------- SQLCommanderPrompt -----------------------------");
            exitStatus = sqlCommanderMain.shellDB();
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
        }
        //System.err.println(" <<< "+exitStatus);
		System.exit(exitStatus);
    }

    public void setPrintInfoDBOnStartup(boolean printInfoDBOnStartup) {
        this.printInfoDBOnStartup = printInfoDBOnStartup;
    }

    public boolean isPrintInfoDBOnStartup() {
        return printInfoDBOnStartup;
    }

	public void setQuietExport(boolean quietExport) {
		this.quietExport = quietExport;
	}

	public boolean isQuietExport() {
		return quietExport;
	}

    protected Connection getConnection() throws IllegalStateException, SQLException {
        Connection conn = null;
        try {
            logger.info("getConnection: ...try get Connection (using " + connectionProperties + ")for Create DB.");
            Class.forName(connectionProperties.getProperty(PARAM_CONNECTION_JDBC_CLASS_DRIVER)).newInstance();
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException(ex.getMessage());
        } catch (InstantiationException ex) {
            throw new IllegalStateException(ex.getMessage());
        } catch (IllegalAccessException ex) {
            throw new IllegalStateException(ex.getMessage());
        }

        logger.info("getConnection:Ok, Loaded JDBC Driver.");
        String urlConnection = connectionProperties.getProperty("jdbc.url");

        if (urlConnection.contains("${db.name}")) {
            urlConnection = urlConnection.replace("${db.name}", connectionProperties.getProperty("db.name"));
            logger.info("getConnection:replacement for variable db.name, now urlConnection=" + urlConnection);
        }

        conn = DriverManager.getConnection(
                urlConnection,
                connectionProperties.getProperty(PARAM_CONNECTION_JDBC_USER),
                connectionProperties.getProperty(PARAM_CONNECTION_JDBC_PASSWORD));

        logger.info("getConnection:OK Connected to DB.");
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
                if (sqlInput.trim().toLowerCase().startsWith("exit")) {
                    break;
                } else if (sqlInput.trim().toLowerCase().equalsIgnoreCase("!help")) {
                    if (prinToConsole) {
                        System.out.println("\t!help    Prints this helpful help to get help ;)");
						System.out.println("\t!dbinfo  Prints DB metadata");
                        System.out.print(promt1);
                    }
                    continue;
                } else if (sqlInput.trim().toLowerCase().equalsIgnoreCase("!dbinfo")) {
                    printDBInfo(conn,connectionProperties.getProperty(PARAM_CONNECTION_JDBC_CATALOG),connectionProperties.getProperty(PARAM_CONNECTION_JDBC_SCHEMMA));
                    if (prinToConsole) {
                        System.out.println("");
                        System.out.print(promt1);
                    }
                    continue;
                } else if (sqlInput.trim().length() == 0 || sqlInput.startsWith("--")) {
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
							logger.finest("  QUERY: ->"+fullSql+"<-");
                            resultExecution = sexec.execute(fullSql);
							logger.finest(" RESULT: "+resultExecution);
                            rs = resultExecution ? sexec.getResultSet() : null;
                        }

                        if (resultExecution && rs != null) {
							
							rsmd = rs.getMetaData();
							numberOfColumns = rsmd.getColumnCount();
							if(printMetadata){
								for (int j = 0; j < numberOfColumns; j++) {								
									System.out.print((j > 0 ? fieldSeprator : "") + rsmd.getColumnClassName(j + 1) );									
								}
								System.out.println();
								for (int j = 0; j < numberOfColumns; j++) {
									int prc = rsmd.getPrecision(j+1);
									int sca = rsmd.getScale    (j+1);
									StringBuilder sbPreSca = new StringBuilder();

									if(prc>0){
										sbPreSca.append("(");
										sbPreSca.append(prc);										
										if(sca>0){
											sbPreSca.append(",");
											sbPreSca.append(sca);
										} 
										sbPreSca.append(")");
									}

									System.out.print((j > 0 ? fieldSeprator : "") + rsmd.getColumnTypeName(j + 1).toUpperCase() + sbPreSca );									
								}
								if (prinToConsole) {           
									System.out.print("\n--------------\n");
								}else {
									System.out.println();
								}
							}
							if(printHeaders){
								for (int j = 0; j < numberOfColumns; j++) {																
									System.out.print((j > 0 ? fieldSeprator : "") + rsmd.getColumnLabel(j + 1) );
								}
								if (prinToConsole) {                                
									System.out.print("\n--------------\n");
								} else {
									System.out.println();
								}
							}
                            int numRows;
                            for (numRows = 0; rs.next(); numRows++) {                                                                
                                for (int j = 0; j < numberOfColumns; j++) {	
									Object o = rs.getObject(j + 1);
									//logger.finest(" DATA: ->"+o+"<-");
									if (o == null) {
										System.out.print((j > 0 ? fieldSeprator + "NULL" : "NULL"));
									} else if (	o.getClass().equals(java.sql.Timestamp.class) || 
												o.getClass().equals(java.sql.Time.class     ) ) {
										System.out.print((j > 0 ? fieldSeprator : "") + stringDelimitator + df_datetime.format(o) + stringDelimitator);
									} else if (	o.getClass().equals(java.sql.Date.class     ) ) {
										System.out.print((j > 0 ? fieldSeprator : "") + stringDelimitator + df_date.format(o) + stringDelimitator);
									} else if (o.getClass().equals(String.class)) {
										System.out.print((j > 0 ? fieldSeprator : "") + stringDelimitator + o.toString() + stringDelimitator);
									} else {
										System.out.print((j > 0 ? fieldSeprator : "") + o.toString());
									}

                                }                                
								System.out.println();
                            }
                            rs.close();
                            if (prinToConsole) {                                
								System.out.print("--------------\n");
								System.out.print(numRows+" rows.\n");
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
		boolean prinToConsole = false;
		boolean repeatInput   = false;
		
		int exitStatus = -1;
		try{
			logger.info("shellDB: System.in.markSupported="+System.in.markSupported());			
			logger.info("shellDB: System.in.available    ="+System.in.available());			
			
			if(quietExport){
				repeatInput   = false;
				prinToConsole = false;
			} else {
				if(System.in.available()>0){
					// IS PIPED STREAM
					repeatInput   = true;
					prinToConsole = true;
				} else {
					// IS REGULAR LIVE CONSOLE STDIN
					repeatInput   = false;
					prinToConsole = true;
				}				
			}
		}catch(Exception e){
			logger.log(Level.SEVERE, "->shellDB:", e);
		}
        logger.info("shellDB: --------------");

        Connection conn = null;
        int errCounter=0;
        try {
            conn = getConnection();
            logger.info("shellDB:OK, the DB exist !!");
						
			logger.info("shellDB:Ready, Now read from stdin(is pipe?"+repeatInput+"), prinToConsole="+prinToConsole+", quietExport="+quietExport+", connectionForInit=" + conn);
            errCounter = executeScriptFrom(is, rdbms, conn, prinToConsole, repeatInput );
            logger.info("-> executeScriptFrom: errCounter="+errCounter);
            if(errCounter == 0){
                exitStatus = 0;
            } else{
                exitStatus = 4;
            }
            logger.info("-> EOF stdin, end");
			
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

	private static void printSplash() {
		Properties vp=loadVersionProperties();
        System.err.println("\t----------------------------- Tracktopell : SQLCommander -----------------------------");      
        System.err.println("\t------------------ https://github.com/tracktopell/SQLCommander.git -------------------");
		System.err.println("\tBUILD  : \t"+vp.getProperty(BUILT_TIMESTAMP));
		System.err.println("\tVERSION: \t"+vp.getProperty(PROJECT_VERSION));        
    }
    private static void printUssage() {		
		System.err.println();
		System.err.println("     Full usage:\t");
        System.err.println("\t  -driverClass jdbc.class.name.driver             -url \"jdbc:rdbs://host:port/DATABASE\" -user USER -password SECRET ");
		System.err.println("\t                                                  [-catalog CATALOG]   [-schemma SCHEMMA]");
		System.err.println("\t                                                  [-printDBInfoOnStartup true|false]   [-continueWithErrors true|false] ");
		System.err.println("\t                                                  [-fs \"|\"]   [-sd \"'\"]   [-q true|false]");
		System.err.println("\t                                                  [-M  true|false]   [-H true|false]  [-l FINE|INFO|WARRINIG]");
		System.err.println();
		System.err.println("for CSV export usage:\t");
        System.err.println("\t  -driverClass jdbc.class.name.driver             -url \"jdbc:rdbs://host:port/DATABASE\"       -user USER -password SECRET -fs \",\" -sd \"\" -q true -M false -H false");
		System.err.println();
        System.err.println("   Common usage:\t");
        System.err.println("\t  -driverClass com.mysql.jdbc.Driver                         -url \"jdbc:mysql://{host}:3306/DATABASE\"      -user USER -password SECRET");		
		System.err.println("\t  -driverClass oracle.jdbc.driver.OracleDriver               -url \"jdbc:oracle:thin:@{host}:1521:DATABASE\" -user USER -password SECRET");
		System.err.println("\t  -driverClass org.apache.derby.jdbc.ClientDriver            -url \"jdbc:derby://{host}:1527/DATABASE\"      -user USER -password SECRET");
		System.err.println("\t  -driverClass org.postgresql.Driver                         -url \"jdbc:postgresql://{host}:5432/DATABASE\" -user USER -password SECRET");
		System.err.println("\t  -driverClass com.microsoft.sqlserver.jdbc.SQLServerDriver  -url \"jdbc:sqlserver://{host}:1600;databaseName=DB\" -user USER -password SECRET");
		System.err.println("\t  -driverClass com.ibm.as400.access.AS400JDBCDriver          -url \"jdbc:as400://{host};[libraries={DATABASE};]\"  -user USER -password SECRET");
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
