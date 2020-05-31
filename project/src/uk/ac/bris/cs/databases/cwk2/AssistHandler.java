package uk.ac.bris.cs.databases.cwk2;
import uk.ac.bris.cs.databases.api.Result;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author dukehan
 */
public class AssistHandler {

    public boolean isUsernameExist(String username, Connection c){

        try (PreparedStatement p = c.prepareStatement(
                "SELECT * FROM Person WHERE username = ? "
        )) {
            p.setString(1, username);
            ResultSet r = p.executeQuery();
            if (r.next() && r.getString("username")!=null) {
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return false;

    }

    public boolean isTopicExist(int topicId, Connection c){

        try (PreparedStatement p = c.prepareStatement(
                "SELECT * FROM Topic WHERE id = ? "
        )) {
            p.setInt(1, topicId);
            ResultSet r = p.executeQuery();
            if (r.next() && r.getInt("id") > 0) {
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return false;

    }

    public boolean isForumExist(int forumId, Connection c){

        try (PreparedStatement p = c.prepareStatement(
                "SELECT * FROM Forum WHERE id = ? "
        )) {
            p.setInt(1, forumId);
            ResultSet r = p.executeQuery();
            if (r.next() && r.getInt("id") > 0) {
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return false;

    }

    public int getTopicId(String title,Connection c){

        int topicId = -1;
        try (PreparedStatement p = c.prepareStatement(
                "SELECT id FROM Topic WHERE title = ? "
        )) {
            p.setString(1, title);
            ResultSet r = p.executeQuery();
            if (r.next()) {
                topicId = r.getInt("id");
                return topicId;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return topicId;
        }
        return topicId;

    }

}
