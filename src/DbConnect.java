import java.sql.*;


public class DbConnect {

	private Connection con;
	private Statement st;
	private ResultSet rs;
	
	public DbConnect(){
		try{
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306/db_data_clean", "root", "");
			st = con.createStatement();
			//System.out.println("Connected");
		}
		catch(Exception ex){
			System.out.println("ERROR: "+ ex);
		}
	}
	
	public ResultSet getRawData(){
		try{
			String query = "select * from tb_main_data";
			rs = st.executeQuery(query);
			//System.out.println("Records from database:");
		}
		catch(Exception ex){
			System.out.println("ERROR: " + ex);
		}
		return rs;
	}
	
	public DbDataPack getData(String country, String capital, String city){
		DbDataPack dbDataPack = new DbDataPack();
		try{
			String query = "select * from tb_master_data where rec_country = \"" + country + "\" and rec_capital = \"" + capital + "\" and rec_city = \""+city + "\"";
			rs = st.executeQuery(query);
			if(rs.next()){
				dbDataPack.country = rs.getString("rec_country");
				dbDataPack.capital = rs.getString("rec_capital");
				dbDataPack.city = rs.getString("rec_city");
				dbDataPack.conference = rs.getString("rec_conference");
			}
			//System.out.println("Record from database:");
		}
		catch(Exception ex){
			System.out.println("ERROR: " + ex);
		}
		return dbDataPack;
	}
	
	public int getCountryCapitalData(String country, String capital) throws SQLException{
		try{
			String query = "select * from tb_master_data where rec_country = \"" + country + "\" and rec_capital = \"" + capital + "\"";
			rs = st.executeQuery(query);
			//System.out.println("Record from database:");
		}
		catch(Exception ex){
			System.out.println("ERROR: " + ex);
		}
		if(rs.next()){
			return 1;
		}
		else{
			return 0;
		}
	}
	
	public int getCountryCityData(String country, String city) throws SQLException{
		try{
			String query = "select * from tb_master_data where rec_country = \"" + country + "\" and rec_city = \""+city + "\"";
			rs = st.executeQuery(query);
			//System.out.println("Record from database:");
		}
		catch(Exception ex){
			System.out.println("ERROR: " + ex);
		}
		if(rs.next()){
			return 1;
		}
		else{
			return 0;
		}
	}
	
	public int getCapitalCityData(String capital, String city) throws SQLException{
		try{
			String query = "select * from tb_master_data where rec_capital = \"" + capital + "\" and rec_city = \""+city + "\"";
			rs = st.executeQuery(query);
			//System.out.println("Record from database:");
		}
		catch(Exception ex){
			System.out.println("ERROR: " + ex);
		}
		if(rs.next()){
			return 1;
		}
		else{
			return 0;
		}
	}
	
	public boolean addMasterData(String country, String capital, String city, String conference) throws SQLException{
		try{
			String query = "select * from tb_master_data where rec_country = \"" + country + "\" and rec_capital = \"" + capital + "\" and rec_city = \""+city + "\"";
			rs = st.executeQuery(query);
		}
		catch(Exception e){
			System.out.println("ERROR: "+ e);
			return false;
		}
		
		if(!rs.next()){
			try{
				String query = "insert into tb_master_data(rec_country, rec_capital, rec_city, rec_conference) values(\"" + country + "\",\"" + capital + "\",\"" + city + "\",\"" + conference + "\")";
				int i = st.executeUpdate(query);
				if( i > 0){
					return true;
				}
				else{
					return false;
				}
			}
			catch(Exception ex){
				System.out.println("ERROR: " + ex);
				return false;
			}
		}
		else{
			//System.out.println("Record already in database");
			return true;
		}
	}
	
	public boolean changeData(int id, String country, String capital, String city, String conference){
		try{
			String query = "update tb_main_data set per_country = '" + country + "', per_capital = '" + capital + "', per_city = '" + city + "', per_conference = '" + conference + "' where per_id = " + id;
			//String query = "insert into tb_master_data(rec_country, rec_capital, rec_city, rec_conference) values(\"" + country + "\",\"" + capital + "\",\"" + city + "\",\"" + conference + "\")";
			int i = st.executeUpdate(query);
			if( i > 0){
				return true;
			}
			else{
				return false;
			}
		}
		catch(Exception ex){
			System.out.println("ERROR: " + ex);
			return false;
		}
	}
	
}
