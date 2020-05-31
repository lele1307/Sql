package uk.ac.bris.cs.databases.cwk2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import uk.ac.bris.cs.databases.api.APIProvider;
import uk.ac.bris.cs.databases.api.ForumSummaryView;
import uk.ac.bris.cs.databases.api.ForumView;
import uk.ac.bris.cs.databases.api.Result;
import uk.ac.bris.cs.databases.api.PersonView;
import uk.ac.bris.cs.databases.api.SimplePostView;
import uk.ac.bris.cs.databases.api.SimpleTopicSummaryView;
import uk.ac.bris.cs.databases.api.TopicView;

/**
 *
 * @author csxdb
 */
public class API implements APIProvider {

    private final Connection c;

    public API(Connection c) {
        this.c = c;
    }

    /* predefined methods */

    @Override
    public Result<Map<String, String>> getUsers() {
        try (Statement s = c.createStatement()) {
            ResultSet r = s.executeQuery("SELECT name, username FROM Person");

            Map<String, String> data = new HashMap<>();
            while (r.next()) {
                data.put(r.getString("username"), r.getString("name"));
            }

            return Result.success(data);
        } catch (SQLException ex) {
            return Result.fatal("database error - " + ex.getMessage());
        }
    }

    @Override
    public Result addNewPerson(String name, String username, String studentId) {
        if (studentId != null && studentId.equals("")) {
            return Result.failure("StudentId can be null, but cannot be the empty string.");
        }
        if (name == null || name.equals("")) {
            return Result.failure("Name cannot be empty.");
        }
        if (username == null || username.equals("")) {
            return Result.failure("Username cannot be empty.");
        }

        try (PreparedStatement p = c.prepareStatement(
            "SELECT count(1) AS c FROM Person WHERE username = ?"
        )) {
            p.setString(1, username);
            ResultSet r = p.executeQuery();

            if (r.next() && r.getInt("c") > 0) {
                return Result.failure("A user called " + username + " already exists.");
            }
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }

        try (PreparedStatement p = c.prepareStatement(
            "INSERT INTO Person (name, username, stuId) VALUES (?, ?, ?)"
        )) {
            p.setString(1, name);
            p.setString(2, username);
            p.setString(3, studentId);
            p.executeUpdate();

            c.commit();
        } catch (SQLException e) {
            try {
                c.rollback();
            } catch (SQLException f) {
                return Result.fatal("SQL error on rollback - [" + f +
                "] from handling exception " + e);
            }
            return Result.fatal(e.getMessage());
        }

        return Result.success();
    }

    /* level 1 */

    @Override
    public Result<PersonView> getPersonView(String username) {
        String personName = null;
        String personUsername = null;
        String stuId = null;
        PersonView personView;
        if ("".equals(username)){
            return Result.failure("Username cannot be empty.");
        }
        try(PreparedStatement p = c.prepareStatement(
                "SELECT * FROM Person WHERE username = ?"
        )){
            p.setString(1,username);
            ResultSet r = p.executeQuery();
            while(r.next()) {
                personName = r.getString("name");
                personUsername = r.getString("username");
                stuId = r.getString("stuId");
            }
            if (personUsername==null){
                return Result.failure("A user called" + username + "is not exists.");
            }
            if (stuId==null){
                stuId = "";
            }
            personView = new PersonView(personName,personUsername,stuId);
        } catch (SQLException ex){
            return Result.fatal(ex.getMessage());
        }
        return Result.success(personView);
    }

    @Override
    public Result<List<ForumSummaryView>> getForums() {
        try (Statement s = c.createStatement()) {
            ResultSet r = s.executeQuery("SELECT * FROM Forum ORDER BY title");

            List<ForumSummaryView> forumSummaryViewList = new ArrayList<>();
            while (r.next()) {
                ForumSummaryView forumSummaryView = new ForumSummaryView(r.getInt("id"),r.getString("title"));
                forumSummaryViewList.add(forumSummaryView);
            }
            return Result.success(forumSummaryViewList);
        } catch (SQLException ex) {
            return Result.fatal("database error - " + ex.getMessage());
        }
    }

    @Override
    public Result<Integer> countPostsInTopic(int topicId) {
        Integer postIn = null;
        try(PreparedStatement p = c.prepareStatement(
                "SELECT COUNT(*) FROM Post INNER JOIN Topic T on Post.topicId = T.id WHERE topicId = ?"
        )){
            p.setInt(1,topicId);
            ResultSet r = p.executeQuery();
            if (r.next()) {
                postIn = r.getInt(1);
            }
        } catch (SQLException ex){
            return Result.fatal(ex.getMessage());
        }
        if (postIn==null){
            return Result.failure("Count Posts In Topic Error!");
        }
        return Result.success(postIn);
    }

    @Override
    public Result<TopicView> getTopic(int topicId) {
        TopicView topicView;
        int tId = -1;
        String title = null;
        String author;
        String text;
        String postAt;
        int count = 0;
        List<SimplePostView> simplePostViewList = new ArrayList<>();
        try(PreparedStatement p = c.prepareStatement(
                "SELECT topicId,title,author,text,time FROM Post INNER JOIN Topic T on T.id = topicId WHERE T.id=?"
        )){
            p.setInt(1,topicId);
            ResultSet r = p.executeQuery();
            while(r.next()) {
                count++;
                tId = r.getInt("topicId");
                title = r.getString("title");
                author = r.getString("author");
                text = r.getString("text");
                DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                postAt = sdf.format(r.getDate("time"));
                System.out.println(count+"  "+tId+"  "+title+" "+author+"  "+text+"  "+postAt);
                SimplePostView n = new SimplePostView(count,author,text,postAt);
                simplePostViewList.add(n);
            }
            if (tId==-1){
                return Result.failure("A Topic Id called" + topicId + "is not exists.");
            }
        } catch (SQLException ex){
            return Result.fatal(ex.getMessage());
        }
        topicView = new TopicView(topicId,title,simplePostViewList);
        return Result.success(topicView);
    }

    /* level 2 */

    @Override
    public Result createForum(String title) {
        title = title.trim();
        if (title == null || "".equals(title)){
            return Result.failure("Forum Title can be null and cannot be the empty string.");
        }
        if (title.length()>100){
            return Result.failure("Topic Title is too long!");
        }
        try (PreparedStatement p = c.prepareStatement(
                "SELECT count(1) AS c FROM Forum WHERE title = ?"
        )) {
            p.setString(1, title);
            ResultSet r = p.executeQuery();

            if (r.next() && r.getInt("c") > 0) {
                return Result.failure("A Forum Title called " + title + " already exists.");
            }
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }

        try (PreparedStatement p = c.prepareStatement(
                "INSERT INTO Forum (title) VALUES (?)"
        )) {
            p.setString(1, title);
            p.executeUpdate();

            c.commit();
        } catch (SQLException e) {
            try {
                c.rollback();
            } catch (SQLException f) {
                return Result.fatal("SQL error on rollback - [" + f +
                        "] from handling exception " + e);
            }
            return Result.fatal(e.getMessage());
        }
        return Result.success();
    }

    @Override
    public Result<ForumView> getForum(int id) {
        ForumView forumView;
        int forumId = -1;
        String title = null;
        List<SimpleTopicSummaryView> simpleTopicSummaryViewList = new ArrayList<>();
        try (PreparedStatement p = c.prepareStatement(
                "SELECT forumId,F.title AS forumTitle,Topic.id AS topicId,Topic.title AS topicTitle FROM Topic RIGHT JOIN Forum F on F.id = forumId WHERE F.id=?"
        )) {
            p.setInt(1, id);
            ResultSet r = p.executeQuery();
            while (r.next()) {
                forumId = r.getInt("forumId");
                title = r.getString("forumTitle");
                if (forumId == 0) {
                    forumView = new ForumView(id, title, simpleTopicSummaryViewList);
                    return Result.success(forumView);
                }
                int topicId = r.getInt("topicId");
                String topicTitle = r.getString("topicTitle");
                System.out.println(forumId + "  " + title + "  " + topicId + "  " + topicTitle);
                simpleTopicSummaryViewList.add(new SimpleTopicSummaryView(topicId, forumId, topicTitle));
            }
            if (forumId == -1) {
                return Result.failure("A forum Id called" + forumId + "is not exists.");
            }
        } catch (SQLException ex) {
            return Result.fatal(ex.getMessage());
        }
        forumView = new ForumView(forumId, title, simpleTopicSummaryViewList);
        return Result.success(forumView);
    }


    /**
     * need add limit conditions
     * */
    @Override
    public Result createPost(int topicId, String username, String text) {
        text = text.trim();
        if (text==null|| "".equals(text)){
            return Result.failure("Post Text can be null and cannot be the empty string.");
        }
        System.out.println(topicId+"  "+username+"  "+text);
        AssistHandler ass = new AssistHandler();
        if (!ass.isUsernameExist(username,c)){
            return Result.failure("Username is not exist!!");
        }
        if (!ass.isTopicExist(topicId,c)){
            return Result.failure("Topic is not exist!!");
        }
        try (PreparedStatement p = c.prepareStatement(
                "INSERT INTO Post (topicId, author, text) VALUES (?, ?, ?)"
        )) {
            p.setInt(1, topicId);
            p.setString(2, username);
            p.setString(3, text);
            p.executeUpdate();
            c.commit();
        } catch (SQLException e) {
            try {
                c.rollback();
            } catch (SQLException f) {
                return Result.fatal("SQL error on rollback - [" + f +
                        "] from handling exception " + e);
            }
            return Result.fatal(e.getMessage());
        }
        return Result.success();
    }


    /* level 3 */
    @Override
    public Result createTopic(int forumId, String username, String title, String text) {
        title = title.trim();
        text = text.trim();
        if ("".equals(title) || title==null){
            return Result.failure("Topic Title cannot be null and cannot be the empty string.");
        }
        if ("".equals(text) || text==null){
            return Result.failure("Topic Text cannot be null and cannot be the empty string.");
        }
        if (title.length()>100){
            return Result.failure("Topic Title is too long!");
        }
        AssistHandler ass = new AssistHandler();
        if (!ass.isUsernameExist(username,c)){
            return Result.failure("Username is not exist!!");
        }
        if (!ass.isForumExist(forumId,c)){
            return Result.failure("Forum is not exist!!");
        }

        try (PreparedStatement p = c.prepareStatement(
                "INSERT INTO Topic (forumId, title) VALUES (?, ?)"
        )) {
            p.setInt(1, forumId);
            p.setString(2, title);
            p.executeUpdate();
            c.commit();
        } catch (SQLException e) {
            try {
                c.rollback();
            } catch (SQLException f) {
                return Result.fatal("SQL error on rollback - [" + f +
                        "] from handling exception " + e);
            }
            return Result.fatal(e.getMessage());
        }
        int topicId = ass.getTopicId(title,c);
        return createPost(topicId,username,text);
    }

}
