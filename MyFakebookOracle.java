package project2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;
import java.sql.PreparedStatement;

public class MyFakebookOracle extends FakebookOracle {
	
	static String prefix = "ethanjyx.";
	
	// You must use the following variable as the JDBC connection
	Connection oracleConnection = null;
	
	// You must refer to the following variables for the corresponding tables in your database
	String cityTableName = null;
	String userTableName = null;
	String friendsTableName = null;
	String currentCityTableName = null;
	String hometownCityTableName = null;
	String programTableName = null;
	String educationTableName = null;
	String eventTableName = null;
	String participantTableName = null;
	String albumTableName = null;
	String photoTableName = null;
	String coverPhotoTableName = null;
	String tagTableName = null;
	
	
	// DO NOT modify this constructor
	public MyFakebookOracle(String u, Connection c) {
		super();
		String dataType = u;
		oracleConnection = c;
		// You will use the following tables in your Java code
		cityTableName = prefix+dataType+"_CITIES";
		userTableName = prefix+dataType+"_USERS";
		friendsTableName = prefix+dataType+"_FRIENDS";
		currentCityTableName = prefix+dataType+"_USER_CURRENT_CITY";
		hometownCityTableName = prefix+dataType+"_USER_HOMETOWN_CITY";
		programTableName = prefix+dataType+"_PROGRAMS";
		educationTableName = prefix+dataType+"_EDUCATION";
		eventTableName = prefix+dataType+"_USER_EVENTS";
		albumTableName = prefix+dataType+"_ALBUMS";
		photoTableName = prefix+dataType+"_PHOTOS";
		tagTableName = prefix+dataType+"_TAGS";
	}
	//top kek
	
	@Override
	// ***** Query 0 *****
	// This query is given to your for free;
	// You can use it as an example to help you write your own code
	//
	public void findMonthOfBirthInfo() throws SQLException{ 
		ResultSet rst = null; 
		PreparedStatement getMonthCountStmt = null;
		PreparedStatement getNamesMostMonthStmt = null;
		PreparedStatement getNamesLeastMonthStmt = null;
		
		try {
			// Scrollable result set allows us to read forward (using next())
			// and also backward.  
			// This is needed here to support the user of isFirst() and isLast() methods,
			// but in many cases you will not need it.
			// To create a "normal" (unscrollable) statement, you would simply call
			// stmt = oracleConnection.prepareStatement(sql);
			//
			String getMonthCountSql = "select count(*), month_of_birth from " +
				userTableName +
				" where month_of_birth is not null group by month_of_birth order by 1 desc";
			getMonthCountStmt = oracleConnection.prepareStatement(getMonthCountSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			
			// getMonthCountSql is the query that will run
			// For each month, find the number of friends born that month
			// Sort them in descending order of count
			// executeQuery will run the query and generate the result set
			rst = getMonthCountStmt.executeQuery();
			
			this.monthOfMostFriend = 0;
			this.monthOfLeastFriend = 0;
			this.totalFriendsWithMonthOfBirth = 0;
			while(rst.next()) {
				int count = rst.getInt(1);
				int month = rst.getInt(2);
				if (rst.isFirst())
					this.monthOfMostFriend = month;
				if (rst.isLast())
					this.monthOfLeastFriend = month;
				this.totalFriendsWithMonthOfBirth += count;
			}
			
			// Get the month with most friends, and the month with least friends.
			// (Notice that this only considers months for which the number of friends is > 0)
			// Also, count how many total friends have listed month of birth (i.e., month_of_birth not null)
			//
			
			// Get the names of friends born in the "most" month
			String getNamesMostMonthSql = "select user_id, first_name, last_name from " + 
				userTableName + 
				" where month_of_birth = ?";
			getNamesMostMonthStmt = oracleConnection.prepareStatement(getNamesMostMonthSql);
			
			// set the first ? in the sql above to value this.monthOfMostFriend, with Integer type
			getNamesMostMonthStmt.setInt(1, this.monthOfMostFriend);
			rst = getNamesMostMonthStmt.executeQuery();
			while(rst.next()) {
				Long uid = rst.getLong(1);
				String firstName = rst.getString(2);
				String lastName = rst.getString(3);
				this.friendsInMonthOfMost.add(new UserInfo(uid, firstName, lastName));
			}
			
			// Get the names of friends born in the "least" month
			String getNamesLeastMonthSql = "select first_name, last_name, user_id from " + 
				userTableName + 
				" where month_of_birth = ?";
			getNamesLeastMonthStmt = oracleConnection.prepareStatement(getNamesLeastMonthSql);
			getNamesLeastMonthStmt.setInt(1, this.monthOfLeastFriend);
			
			rst = getNamesLeastMonthStmt.executeQuery();
			while(rst.next()){
				String firstName = rst.getString(1);
				String lastName = rst.getString(2);
				Long uid = rst.getLong(3);
				this.friendsInMonthOfLeast.add(new UserInfo(uid, firstName, lastName));
			}
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(rst != null) 
				rst.close();
			
			if(getMonthCountStmt != null)
				getMonthCountStmt.close();
			
			if(getNamesMostMonthStmt != null)
				getNamesMostMonthStmt.close();
			
			if(getNamesLeastMonthStmt != null)
				getNamesLeastMonthStmt.close();
		}
	}

	
	
	@Override
	// ***** Query 1 *****
	// Find information about friend names:
	// (1) The longest last name (if there is a tie, include all in result)
	// (2) The shortest last name (if there is a tie, include all in result)
	// (3) The most common last name, and the number of times it appears (if there is a tie, include all in result)
	//
	public void findNameInfo() throws SQLException { // Query1
        
		
		ResultSet rst = null; 
		PreparedStatement getNamesStmt = null;
		PreparedStatement getCommonStmt = null;
		try {
			String getNamesSql = "SELECT LAST_NAME FROM " + userTableName+ " ORDER BY LENGTH(LAST_NAME) desc";
			getNamesStmt = oracleConnection.prepareStatement(getNamesSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rst = getNamesStmt.executeQuery();
			int lenLong = -3;
			int lenShort = -3;
			
			while(rst.next()) {
				String lastName = rst.getString(1);
				if(rst.isFirst()){
					this.longestLastNames.add(lastName);
					lenLong = lastName.length();
				}
				else if (lastName.length() == lenLong){
					this.longestLastNames.add(lastName);
				}
			}
			while(rst.previous()) {
				String lastName = rst.getString(1);
				if(rst.isLast()){
					this.shortestLastNames.add(lastName);
					lenShort = lastName.length();
				}
				else if (lastName.length() == lenShort){
					this.shortestLastNames.add(lastName);
				}
			}
			
			
			String getCommonSql = "SELECT LAST_NAME, COUNT(LAST_NAME)"
					+ "AS NAMECOUNT FROM " + userTableName+ " GROUP BY LAST_NAME ORDER BY NAMECOUNT desc";
			getCommonStmt = oracleConnection.prepareStatement(getCommonSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rst = getCommonStmt.executeQuery();
			while(rst.next()) {
				if(rst.isFirst()){
					this.mostCommonLastNames.add(rst.getString(1));
					this.mostCommonLastNamesCount = rst.getInt(2);
				}
			}
		} catch (SQLException e) {
					System.err.println(e.getMessage());
					// can do more things here
					
					throw e;		
				} finally {
					// Close statement and result set
					if(rst != null) 
						rst.close();
					
					if(getNamesStmt != null)
						getNamesStmt.close();
					
					if(getCommonStmt != null)
						getCommonStmt.close();
				}

	}
	@Override
	// ***** Query 2 *****
	// Find the user(s) who have strictly more than 80 friends in the network
	//
	// Be careful on this query!
	// Remember that if two users are friends, the friends table
	// only contains the pair of user ids once, subject to 
	// the constraint that user1_id < user2_id
	//
	public void popularFriends() throws SQLException {
		

		ResultSet rst = null; 
		PreparedStatement getStmt = null;
		
		try
		{
			
			String getSql = "SELECT u.USER_ID, u.FIRST_NAME, u.LAST_NAME FROM " + userTableName 
					+ " u JOIN (SELECT h.USER1_ID, COUNT(h.USER1_ID) AS NUMFRIENDS FROM " + "(SELECT f.USER1_ID, f.USER2_ID FROM "+
					friendsTableName+" f UNION ALL SELECT g.USER2_ID, g.USER1_ID FROM "+ friendsTableName + 
					" g) h GROUP BY USER1_ID) i ON i.USER1_ID = u.USER_ID AND i.NUMFRIENDS > 80";
					
					/*"SELECT u.USER_ID, COUNT(u.USER_ID) AS NUMFRIENDS FROM " + userTableName + " u JOIN (SELECT f.USER1_ID, f.USER2_ID FROM "+
					friendsTableName+" f UNION ALL SELECT g.USER1_ID, g.USER2_ID FROM "+ friendsTableName + 
					" g) uni ON u.user_id = uni.user1_id GROUP BY USER_ID";*/
								
			getStmt = oracleConnection.prepareStatement(getSql);
			rst = getStmt.executeQuery();
			
			int count = 0;
			while(rst.next()){
				String firstName = rst.getString(2);
				String lastName = rst.getString(3);
				Long uid = rst.getLong(1);
				this.popularFriends.add(new UserInfo(uid, firstName, lastName));
				count++;
			}
			this.countPopularFriends = count;
		}
		catch (SQLException e) 
		{
			System.err.println(e.getMessage());
			
			throw e;		
		} 
		finally 
		{
		}
	}
	 

	@Override
	// ***** Query 3 *****
	// Find the users who still live in their hometowns
	// (I.e., current_city = hometown_city)
	//	
	public void liveAtHome() throws SQLException {
		
		ResultSet rst = null; 
		PreparedStatement getNamesStmt = null;
		try {
			String getNamesSql = "SELECT a.USER_ID, a.FIRST_NAME, a.LAST_NAME FROM " + userTableName + " a INNER JOIN " + 
					"(SELECT " + hometownCityTableName + ".USER_ID FROM " + hometownCityTableName+ " INNER JOIN " +
					currentCityTableName + " ON " + hometownCityTableName + ".USER_ID = " + currentCityTableName + ".USER_ID AND " +
						hometownCityTableName + ".HOMETOWN_CITY_ID = " + currentCityTableName + ".CURRENT_CITY_ID) b ON a.USER_ID = b.USER_ID";
			getNamesStmt = oracleConnection.prepareStatement(getNamesSql);
			rst = getNamesStmt.executeQuery();
			while(rst.next()){
				this.liveAtHome.add(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
				this.countLiveAtHome++;
			}
			
		} 		
		catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(rst != null) 
				rst.close();
			
			if(getNamesStmt != null)
				getNamesStmt.close();
		}
	}
	



	@Override
	// **** Query 4 ****
	// Find the top-n photos based on the number of tagged users
	// If there are ties, choose the photo with the smaller numeric PhotoID first
	// 
	public void findPhotosWithMostTags(int n) throws SQLException { 
		
		ResultSet rst = null; 
		ResultSet rst2 = null; 
		PreparedStatement dropView = null;
		PreparedStatement getViewStmt = null;
		PreparedStatement getPhotosStmt = null;
		PreparedStatement getNamesStmt = null;
		try {
			
			String getViewSql = "CREATE VIEW QUANT AS SELECT TAG_PHOTO_ID, TAG_SUBJECT_ID, NUMTAGS FROM" +
					 "(SELECT j.TAG_PHOTO_ID, k.TAG_SUBJECT_ID, j.NUMTAGS FROM (SELECT t.TAG_PHOTO_ID,"
					+ " COUNT(t.TAG_PHOTO_ID) AS NUMTAGS FROM " + 
						tagTableName + " t JOIN " + photoTableName + " p ON t.TAG_PHOTO_ID = p.PHOTO_ID GROUP BY t.TAG_PHOTO_ID"
					+ " ORDER BY NUMTAGS desc) j JOIN " + tagTableName + " k ON j.TAG_PHOTO_ID = k.TAG_PHOTO_ID)";
			String getPhotosSql = "SELECT DISTINCT PHOTO_ID, ALBUM_ID, ALBUM_NAME, PHOTO_CAPTION, PHOTO_LINK FROM "
					+ "(SELECT p.PHOTO_ID, p.ALBUM_ID, a.ALBUM_NAME, p.PHOTO_CAPTION, p.PHOTO_LINK FROM " +
					albumTableName + " a JOIN QUANT q JOIN " +
					photoTableName + " p ON q.TAG_PHOTO_ID = p.PHOTO_ID AND q.NUMTAGS = (SELECT MAX(NUMTAGS) FROM QUANT) "
							+ "ON a.ALBUM_ID = p.ALBUM_ID ORDER BY q.TAG_PHOTO_ID) ORDER BY PHOTO_ID";
			String getNamesSql = "SELECT u.USER_ID, u.FIRST_NAME, u.LAST_NAME, q.NUMTAGS FROM " + userTableName + " u JOIN"
					+ " QUANT q ON q.TAG_SUBJECT_ID = u.USER_ID AND q.NUMTAGS = (SELECT MAX(NUMTAGS) FROM QUANT) ORDER BY q.TAG_PHOTO_ID";
			String dropSql = "DROP VIEW QUANT"; 
			getViewStmt = oracleConnection.prepareStatement(getViewSql);
			getPhotosStmt = oracleConnection.prepareStatement(getPhotosSql);
			getNamesStmt = oracleConnection.prepareStatement(getNamesSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			dropView = oracleConnection.prepareStatement(dropSql);
			
			rst = getViewStmt.executeQuery();
			rst = getPhotosStmt.executeQuery();
			rst2 = getNamesStmt.executeQuery();
			int w = 0;
			while(rst.next() && w < n){
				
				String photoId = rst.getString(1);
				String albumId = rst.getString(2);
				String albumName = rst.getString(3);
				String photoCaption = rst.getString(4);
				String photoLink = rst.getString(5);
					PhotoInfo p = new PhotoInfo(photoId, albumId, albumName, photoCaption, photoLink);
					TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
					int i = 0;
					while(rst2.next() && i < rst2.getInt(4)){
					tp.addTaggedUser(new UserInfo(rst2.getLong(1), rst2.getString(2), rst2.getString(3)));
						i++;
					}
					rst2.previous();
					this.photosWithMostTags.add(tp);
				w++;
			}
			rst = dropView.executeQuery();
				
			
			
		} 		
		catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(rst != null) 
				rst.close();
			
			if(getViewStmt != null)
				getViewStmt.close();
			if(getPhotosStmt != null)
				getPhotosStmt.close();
			if(dropView != null)
				dropView.close();
			if(getNamesStmt != null)
				getNamesStmt.close();
		}
		
		
	}

	
	
	
	@Override
	// **** Query 5 ****
	// Find suggested "match pairs" of friends, using the following criteria:
	// (1) One of the friends is female, and the other is male
	// (2) Their age difference is within "yearDiff"
	// (3) They are not friends with one another
	// (4) They should be tagged together in at least one photo
	//
	// You should up to n "match pairs"
	// If there are more than n match pairs, you should break ties as follows:
	// (i) First choose the pairs with the largest number of shared photos
	// (ii) If there are still ties, choose the pair with the smaller user_id for the female
	// (iii) If there are still ties, choose the pair with the smaller user_id for the male
	//
	public void matchMaker(int n, int yearDiff) throws SQLException { 
		
		
		ResultSet rst = null; 
		ResultSet rst2 = null; 
		ResultSet rst3 = null; 
		PreparedStatement getYViewStmt = null;
		PreparedStatement getFViewStmt = null;
		PreparedStatement getPViewStmt = null;
		PreparedStatement getCountStmt = null;
		
		PreparedStatement getFNameStmt = null;
		PreparedStatement getMNameStmt = null;
		PreparedStatement getPhotoStmt = null;
		
		PreparedStatement dropMViewStmt = null;
		PreparedStatement dropYViewStmt = null;
		PreparedStatement dropFViewStmt = null;
		PreparedStatement dropPViewStmt = null;
		try{
			//YEARDIFF GAVE AN ERROR WHEN TRYING TO USE "?" AND PASS AS PARAMETER
			String getYViewSql = "CREATE VIEW Comp AS SELECT DISTINCT f.user_id AS user1_id, m.user_id AS user2_id FROM " + userTableName + 
					" f, " + userTableName + " m WHERE f.GENDER = 'female' AND m.GENDER = 'male' AND ((f.YEAR_OF_BIRTH "
							+ "- m.YEAR_OF_BIRTH <= " + yearDiff + " AND f.YEAR_OF_BIRTH - m.YEAR_OF_BIRTH >= 0)"
									+ " OR (m.YEAR_OF_BIRTH "
							+ "- f.YEAR_OF_BIRTH <= " + yearDiff + " AND m.YEAR_OF_BIRTH - f.YEAR_OF_BIRTH >= 0))";
			String getFViewSql = "CREATE VIEW Notf AS SELECT c.user1_id, c.user2_id FROM COMP c MINUS "
					+ "(SELECT f.USER1_ID, f.USER2_ID FROM "+
							friendsTableName+" f UNION ALL SELECT g.USER2_ID, g.USER1_ID FROM "+ friendsTableName + 
							" g)";
			
			String getPViewSql = "CREATE VIEW TAGGED AS SELECT a.TAG_SUBJECT_ID AS first, b.TAG_SUBJECT_ID AS sec, a.TAG_PHOTO_ID"
					+ " FROM  "
					+ tagTableName + " a, " +tagTableName + " b WHERE a.TAG_PHOTO_ID = b.TAG_PHOTO_ID AND a.TAG_SUBJECT_ID !="
							+ " b.TAG_SUBJECT_ID";
			String getCountSql = "CREATE VIEW Match AS SELECT u.first, u.sec, u.TAG_PHOTO_ID, u.shared FROM "
					+ "(SELECT h.first, h.sec, h.TAG_PHOTO_ID, COUNT(h.sec) AS shared FROM TAGGED h GROUP BY h.first, h.sec, h.TAG_PHOTO_ID) u "
					+ "JOIN Notf n ON n.user1_id = u.first AND n.user2_id = u.sec ORDER BY u.shared, u.first, u.sec";
			
			String getFNameSql = "SELECT f.user_id, f.FIRST_NAME, f.LAST_NAME, f.YEAR_OF_BIRTH FROM MATCH q JOIN "
					+ userTableName +
					" f ON q.first = f.user_id";
			String getMNameSql = "SELECT m.user_id, m.FIRST_NAME, m.LAST_NAME, m.YEAR_OF_BIRTH FROM MATCH q JOIN "
					+ userTableName +
					" m ON q.sec = m.user_id";
			
			String getPhotosSql = "SELECT DISTINCT PHOTO_ID, ALBUM_ID, ALBUM_NAME, PHOTO_CAPTION, PHOTO_LINK, shared FROM "
					+ "(SELECT p.PHOTO_ID, p.ALBUM_ID, a.ALBUM_NAME, p.PHOTO_CAPTION, p.PHOTO_LINK, m.shared FROM " +
					albumTableName + " a JOIN MATCH m JOIN " +
					photoTableName + " p ON m.TAG_PHOTO_ID = p.PHOTO_ID "
							+ "ON a.ALBUM_ID = p.ALBUM_ID ORDER BY m.first, m.sec)";
			String dropYViewSql = "DROP VIEW Comp";
			String dropFViewSql = "DROP VIEW Notf";
			String dropPViewSql = "DROP VIEW TAGGED";
			String dropMViewSql = "DROP VIEW Match";
			getYViewStmt = oracleConnection.prepareStatement(getYViewSql);
			getFViewStmt = oracleConnection.prepareStatement(getFViewSql);
			getPViewStmt = oracleConnection.prepareStatement(getPViewSql);
			getCountStmt = oracleConnection.prepareStatement(getCountSql);
			
			getFNameStmt = oracleConnection.prepareStatement(getFNameSql);
			getMNameStmt = oracleConnection.prepareStatement(getMNameSql);
			getPhotoStmt = oracleConnection.prepareStatement(getPhotosSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			dropYViewStmt = oracleConnection.prepareStatement(dropYViewSql);
			dropFViewStmt = oracleConnection.prepareStatement(dropFViewSql);
			dropPViewStmt = oracleConnection.prepareStatement(dropPViewSql);
			dropMViewStmt = oracleConnection.prepareStatement(dropMViewSql);
			
			rst = getYViewStmt.executeQuery();
			rst = getFViewStmt.executeQuery();
			rst = getPViewStmt.executeQuery();
			rst = getCountStmt.executeQuery();
			rst = getFNameStmt.executeQuery();
			rst2 = getMNameStmt.executeQuery();
			rst3 = getPhotoStmt.executeQuery();
			int count = 0;
			while(rst.next() && rst2.next() && count < n){
				Long fid = rst.getLong(1);
				String ffirst = rst.getString(2);
				String flast = rst.getString(3);
				int fbirth= rst.getInt(4);
				Long mid = rst2.getLong(1);
				String mfirst = rst2.getString(2);
				String mlast = rst2.getString(3);
				int mbirth= rst2.getInt(4);
				MatchPair mp = new MatchPair(fid, ffirst, flast, 
						fbirth, mid, mfirst, mlast, mbirth);
				int c2 = 0;
				while(rst3.next() && c2 < rst3.getInt(6)){
				String sharedPhotoId = rst3.getString(1);
				String sharedPhotoAlbumId = rst3.getString(2);
				String sharedPhotoAlbumName = rst3.getString(3);
				String sharedPhotoCaption = rst3.getString(4);
				String sharedPhotoLink = rst3.getString(5);
				mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId, 
						sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));
				c2++;
				}
				rst3.previous();
				this.bestMatches.add(mp);
				count ++;
			}
			rst = dropMViewStmt.executeQuery();
			rst = dropYViewStmt.executeQuery();
			rst = dropFViewStmt.executeQuery();
			rst = dropPViewStmt.executeQuery();
		}
		
		
		catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(rst != null) 
				rst.close();
			
			if(getYViewStmt != null)
				getYViewStmt.close();
			if(getFViewStmt != null)
				getFViewStmt.close();
			if(getPViewStmt != null)
				getPViewStmt.close();
			if(getCountStmt != null)
				getCountStmt.close();
			if(getFNameStmt != null)
				getFNameStmt.close();
			if(getMNameStmt != null)
				getMNameStmt.close();
			if(getPhotoStmt != null)
				getPhotoStmt.close();
			if(dropMViewStmt != null)
				dropMViewStmt.close();
			if(dropYViewStmt != null)
				dropYViewStmt.close();
			if(dropYViewStmt != null)
				dropYViewStmt.close();
			if(dropFViewStmt != null)
				dropFViewStmt.close();
			if(dropPViewStmt != null)
				dropPViewStmt.close();
		}
		
	}

	
	
	// **** Query 6 ****
	// Suggest friends based on mutual friends
	// 
	// Find the top n pairs of users in the database who share the most
	// friends, but such that the two users are not friends themselves.
	//
	// Your output will consist of a set of pairs (user1_id, user2_id)
	// No pair should appear in the result twice; you should always order the pairs so that
	// user1_id < user2_id
	//
	// If there are ties, you should give priority to the pair with the smaller user1_id.
	// If there are still ties, give priority to the pair with the smaller user2_id.
	//
	@Override
	public void suggestFriendsByMutualFriends(int n) throws SQLException {
		
		/*
		 * This query is too slow and we couldn't find a better method to speed it up. We've commented it out for now
		 * so that you can run the code without it getting in the way.
		 */
		
		/*
		ResultSet rst1 = null; 
		PreparedStatement getStmt1 = null;
		try {
			
			
			String setOfAllFriendships = 
"SELECT a.user_id AS usera_id, b.user_id AS userb_id, a.first_name AS afirst_name, b.first_name AS bfirst_name, a.last_name as alast_name, b.last_name as blast_name "+
"FROM "+userTableName+" a, "+userTableName+" b, "+friendsTableName+" f "+
"WHERE a.user_id = f.user1_id AND b.user_id = f.user2_id AND a.user_id < b.user_id";
			
			String setOfTotal = 
"SELECT aa.user_id AS usera_id, bb.user_id AS userb_id, aa.first_name AS afirst_name, bb.first_name AS bfirst_name, aa.last_name as alast_name, bb.last_name as blast_name "+
"FROM "+userTableName+" aa, "+userTableName+" bb "+
"WHERE aa.user_id < bb.user_id AND NOT EXISTS ("+setOfAllFriendships+" AND aa.user_id = a.user_id AND bb.user_id = b.user_id)";
			
			String getSQL1 = 
"SELECT ff.usera_id, ff.userb_id, ff.afirst_name, ff.bfirst_name, ff.alast_name, ff.blast_name, p.user_id as userf_id, p.first_name as ffirst_name, p.last_name as flast_name "+
"FROM ("+setOfTotal+") ff, "+userTableName+" p, "+friendsTableName+" g0, "+friendsTableName+" g1 "+
"WHERE ((ff.usera_id = g0.user1_id AND p.user_id = g0.user2_id) OR (ff.usera_id = g0.user2_id AND p.user_id = g0.user1_id)) "+
"AND ((ff.userb_id = g1.user1_id AND p.user_id = g1.user2_id) OR (ff.userb_id = g1.user2_id AND p.user_id = g1.user1_id)) ";


			getStmt1 = oracleConnection.prepareStatement(getSQL1);
			rst1 = getStmt1.executeQuery();
		   
			while(rst1.next()){
				
				for(FriendsPair f : this.suggestedFriendsPairs)
				{
					if(f.user1Id == rst1.getLong(1) && f.user2Id == rst1.getLong(2))
					{
						f.addSharedFriend(rst1.getLong(7), rst1.getString(8), rst1.getString(9));
						break;
					}
				}
				
				Long user1_id = rst1.getLong(1);
				String user1FirstName = rst1.getString(3);
				String user1LastName = rst1.getString(5);
				Long user2_id = rst1.getLong(2);
				String user2FirstName = rst1.getString(4);
				String user2LastName = rst1.getString(6);
				FriendsPair p = new FriendsPair(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
				p.addSharedFriend(rst1.getLong(7), rst1.getString(8), rst1.getString(9));
				this.suggestedFriendsPairs.add(p);
			}
			
		} 		
		catch (SQLException e) {
			System.err.println(e.getMessage());
			
			throw e;		
		} finally {
			
			if(rst1 != null) 
				rst1.close();

			if(getStmt1 != null)
				getStmt1.close();
		}*/
	}
	
	
	//@Override
	// ***** Query 7 *****
	// Given the ID of a user, find information about that
	// user's oldest friend and youngest friend
	// 
	// If two users have exactly the same age, meaning that they were born
	// on the same day, then assume that the one with the larger user_id is older
	//
	public void findAgeInfo(Long user_id) throws SQLException {
		
		
		ResultSet rst = null; 
		PreparedStatement getNamesStmt = null;
		
		try {
		String getNamesSql = "SELECT u.user_id, u.first_name, u.last_name FROM  " + userTableName 
				+ " u JOIN (select DISTINCT user2_id FROM (SELECT f.USER1_ID, f.USER2_ID FROM "+
				friendsTableName +" f UNION ALL SELECT g.USER2_ID, g.USER1_ID FROM "+ friendsTableName + " g)" + 
				" where user1_id = ?) j ON u.user_id = j.user2_id ORDER BY u.YEAR_OF_BIRTH DESC, u.MONTH_OF_BIRTH DESC, "
				+ "u.DAY_OF_BIRTH DESC";
			getNamesStmt = oracleConnection.prepareStatement(getNamesSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			getNamesStmt.setLong(1, user_id);
			rst = getNamesStmt.executeQuery();
			while(rst.next()){
				if(rst.isFirst()){
					this.youngestFriend = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
				}
				if(rst.isLast()){
					this.oldestFriend = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
				}
			}
		} 		
		catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(rst != null) 
				rst.close();
			
			if(getNamesStmt != null)
				getNamesStmt.close();
		}
	}
	
	
	@Override
	// ***** Query 8 *****
	// 
	// Find the name of the city with the most events, as well as the number of 
	// events in that city.  If there is a tie, return the names of all of the (tied) cities.
	//
	public void findEventCities() throws SQLException {
		
		ResultSet rst = null; 
		PreparedStatement createViewStmt = null;
		PreparedStatement getFinalStmt = null;
		PreparedStatement dropViewStmt = null;
		try {
			String createViewSql = 
"CREATE VIEW POPULAR_CITIES AS SELECT s.CITY_ID, s.CITY_NAME, s.NUMEVENTS "+
"FROM (SELECT c.CITY_ID, c.CITY_NAME, COUNT(c.CITY_ID) AS NUMEVENTS "+
	  "FROM "+cityTableName+" c, "+eventTableName+" e "+
	  "WHERE c.CITY_ID = e.EVENT_CITY_ID "+
	  "GROUP BY CITY_ID, CITY_NAME) s "+
"ORDER BY 3 DESC";
			String getFinalSql=
"SELECT a.CITY_NAME, a.NUMEVENTS "+
"FROM POPULAR_CITIES a "+
"WHERE a.NUMEVENTS = (SELECT MAX(NUMEVENTS) FROM POPULAR_CITIES)";
			String dropViewSql=
"DROP VIEW POPULAR_CITIES";
			
			createViewStmt = oracleConnection.prepareStatement(createViewSql);
			createViewStmt.executeQuery();
			getFinalStmt = oracleConnection.prepareStatement(getFinalSql);
			rst = getFinalStmt.executeQuery();
			dropViewStmt = oracleConnection.prepareStatement(dropViewSql);
			dropViewStmt.executeQuery();
			while(rst.next()){
				this.eventCount = rst.getInt(2);
				this.popularCityNames.add(rst.getString(1));
			}
			
		} 		
		catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(rst != null) 
				rst.close();

			if(createViewStmt != null)
				createViewStmt.close();
			if(getFinalStmt != null)
				getFinalStmt.close();
			if(dropViewStmt != null)
				dropViewStmt.close();
		}
	}
	
	
	
	@Override
//	 ***** Query 9 *****
	//
	// Find pairs of potential siblings and print them out in the following format:
	//   # pairs of siblings
	//   sibling1 lastname(id) and sibling2 lastname(id)
	//   siblingA lastname(id) and siblingB lastname(id)  etc.
	//
	// A pair of users are potential siblings if they have the same last name and hometown, if they are friends, and
	// if they are less than 10 years apart in age.  Pairs of siblings are returned with the lower user_id user first
	// on the line.  They are ordered based on the first user_id and in the event of a tie, the second user_id.
	//  
	//
	public void findPotentialSiblings() throws SQLException {
		
		
		ResultSet rst = null; 
		PreparedStatement createViewStmt = null;
		PreparedStatement dropViewStmt = null;
		PreparedStatement getStmt = null;
		try {
			String i0 = 
"SELECT a.user_id as user1_id, b.user_id AS user2_id, a.first_name as first_name1, b.first_name as first_name2, a.last_name "+
"FROM "+userTableName+" a, "+userTableName+" b "+
"WHERE a.last_name = b.last_name AND a.user_id < b.user_id AND ABS(a.year_of_birth - b.year_of_birth) < 10";
			
			String i1 = 
"SELECT s.user1_id, s.user2_id, s.first_name1, s.first_name2, s.last_name, h0.hometown_city_id as hometown1_id, h1.hometown_city_id as hometown2_id "+
"FROM ("+i0+") s, USER_HOMETOWN_CITY h0, USER_HOMETOWN_CITY h1 "+
"WHERE s.user1_id = h0.user_id AND s.user2_id = h1.user_id AND h0.hometown_city_id = h1.hometown_city_id";
			
			String createViewSQL = 
"CREATE VIEW USER_MATCHING AS (" + i1 + ")";
			
			String dropViewSQL = 
"DROP VIEW USER_MATCHING";
			
			String getSQL0=
"SELECT m.user1_id, m.user2_id, m.first_name1, m.first_name2, m.last_name, m.hometown1_id, m.hometown2_id "+
"FROM FRIENDS f, USER_MATCHING m "+
"WHERE f.user1_id = m.user1_id AND f.user2_id = m.user2_id "+
"ORDER BY m.user1_id, m.user2_id";

			createViewStmt = oracleConnection.prepareStatement(createViewSQL);
			createViewStmt.executeQuery();
			getStmt = oracleConnection.prepareStatement(getSQL0);
			rst = getStmt.executeQuery();
			dropViewStmt = oracleConnection.prepareStatement(dropViewSQL);
			dropViewStmt.executeQuery();
			
			while(rst.next()){
				Long user1_id = rst.getLong(1);
				String user1FirstName = rst.getString(3);
				String user1LastName = rst.getString(5);
				Long user2_id = rst.getLong(2);
				String user2FirstName = rst.getString(4);
				String user2LastName = rst.getString(5);
				SiblingInfo s = new SiblingInfo(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
				//System.out.println(user1FirstName+" "+user1LastName + "="+user2FirstName+" "+user2LastName +" -> " + rst.getLong(6) + "="+rst.getLong(7));
				this.siblings.add(s);
			}
			
		} 		
		catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(rst != null) 
				rst.close();

			if(getStmt != null)
				getStmt.close();
		}
	}
	
}
