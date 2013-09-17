package edu.anadolu.solr.handler.dataimport;

import edu.anadolu.helper.JsonHelper;
import org.apache.log4j.Logger;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.EntityProcessorBase;
import org.springframework.util.StringUtils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TwitterEntityProcessor extends EntityProcessorBase {

    private static String user = null;
    private static String password = null;

    private volatile TwitterStream stream;

    private FilterQuery filterQuery;

    private String streamType;

    private String oAuthAccessTokenSecret;
    private String oAuthAccessToken;
    private String oAuthConsumerKey;
    private String oAuthConsumerSecret;

    private boolean closed = false;

    int connectionTimeout = 10000;
    int readTimeout = 10000;
    private static final Pattern CHARSET_PATTERN = Pattern.compile(".*?charset=(.*)$", Pattern.CASE_INSENSITIVE);
    public static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static final Logger logger = Logger.getLogger(TwitterEntityProcessor.class);
    Stack<Map<String, Object>> stackTweetDetails = new Stack<Map<String, Object>>();
    GregorianCalendar calendar = new GregorianCalendar();
    public void init(Context context) {
        super.init(context);

        // OAUTH
        oAuthAccessTokenSecret = context.getEntityAttribute("oAuthAccessTokenSecret");
        oAuthAccessToken = context.getEntityAttribute("oAuthAccessToken");
        oAuthConsumerKey = context.getEntityAttribute("oAuthConsumerKey");
        oAuthConsumerSecret = context.getEntityAttribute("oAuthConsumerSecret");

        logger.debug("oAuthAccessTokenSecret : " + oAuthAccessTokenSecret);
        logger.debug("oAuthAccessToken : " + oAuthAccessToken);
        logger.debug("oAuthConsumerKey : " + oAuthConsumerKey);
        logger.debug("oAuthConsumerSecret : " + oAuthConsumerSecret);

        // TYPE
        streamType = context.getEntityAttribute("type") != null ? context.getEntityAttribute("type") : "sample";

        logger.debug("streamType : " + streamType);

        filterQuery = new FilterQuery();
        // COUNT
        try {
            filterQuery.count(Integer.getInteger(context.getEntityAttribute("count") != null ? context.getEntityAttribute("count") : "0"));
        } catch (Exception e) {
            logger.error(e);
        }
        // TRACKS
        String tracks = context.getEntityAttribute("tracks");
        if (tracks != null) {
            filterQuery.track(StringUtils.commaDelimitedListToStringArray(tracks));
        }
        // FOLLOW
        String follow = context.getEntityAttribute("follow");
        if (follow != null) {
            ArrayList<Long> followList = new ArrayList<Long>();
            for (String id : follow.split(",")) {
                followList.add(Long.parseLong(id));
            }
            long[] followArray = new long[followList.size()];
            for (int i = 0; i < followList.size(); i++) {
                followArray[i] = followList.get(i);
            }
            filterQuery.follow(followArray);
        }
        // LOCATİON
        String locations = context.getEntityAttribute("locations");
        if (locations != null) {
            String[] sLocations = StringUtils.commaDelimitedListToStringArray(locations);
            double[][] dLocations = new double[sLocations.length / 2][];
            int dCounter = 0;
            for (int i = 0; i < sLocations.length; i++) {
                double lat = Double.parseDouble(sLocations[i]);
                double lon = Double.parseDouble(sLocations[++i]);
                dLocations[dCounter++] = new double[]{lat, lon};
            }
            filterQuery.locations(dLocations);
        }

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setJSONStoreEnabled(true);

        if (oAuthAccessToken != null && oAuthConsumerKey != null && oAuthConsumerSecret != null && oAuthAccessTokenSecret != null) {
            cb.setOAuthConsumerKey(oAuthConsumerKey)
                    .setOAuthConsumerSecret(oAuthConsumerSecret)
                    .setOAuthAccessToken(oAuthAccessToken)
                    .setOAuthAccessTokenSecret(oAuthAccessTokenSecret);
        } else {
            return;
            //cb.setUser(user).setPassword(password);
        }

        stream = new TwitterStreamFactory(cb.build()).getInstance();

        if (streamType.equals("filter") && filterQuery != null) {
            stream.addListener(new StatusHandler());
            stream.filter(filterQuery);
        } else if (streamType.equals("user")) {
            stream.addListener(new UserStreamListener());
            stream.user();
        } else if (streamType.equals("sample")) {
            stream.addListener(new StatusHandler());
            stream.sample();
        }
    }

    private void reconnect() {

        try {
            stream.cleanUp();
        } catch (Exception e) {
            logger.debug("failed to cleanup after failure", e);
        }
        try {
            stream.shutdown();
        } catch (Exception e) {
            logger.debug("failed to shutdown after failure", e);
        }


        try {
            ConfigurationBuilder cb = new ConfigurationBuilder();
            if (oAuthAccessToken != null && oAuthConsumerKey != null && oAuthConsumerSecret != null && oAuthAccessTokenSecret != null) {
                cb.setOAuthConsumerKey(oAuthConsumerKey)
                        .setOAuthConsumerSecret(oAuthConsumerSecret)
                        .setOAuthAccessToken(oAuthAccessToken)
                        .setOAuthAccessTokenSecret(oAuthAccessTokenSecret);
            }
            stream = new TwitterStreamFactory(cb.build()).getInstance();
            stream.addListener(new StatusHandler());

            if (streamType.equals("filter") || filterQuery != null) {
                stream.filter(filterQuery);

            } else if (streamType.equals("user")) {
                stream.user();
            } else {
                stream.sample();
            }
        } catch (Exception e) {

            logger.warn("failed to connect after failure, throttling", e);

        }

    }

    @Override
    public Map<String, Object> nextRow() {
        while (true) {
            if (closed)
                return null;
            if (stackTweetDetails.isEmpty()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.error(e);
                }
            } else
                return stackTweetDetails.pop();
        }
    }

    @Override
    public Map<String, Object> nextModifiedRowKey() {
        return null;
    }

    @Override
    public Map<String, Object> nextDeletedRowKey() {
        return null;
    }

    @Override
    public Map<String, Object> nextModifiedParentRowKey() {
        return null;
    }

    @Override
    public void destroy() {
        closed = true;
        logger.info("closing twitter stream river");
        if (stream != null) {
            stream.cleanUp();
            stream.shutdown();
        }
        System.out.println("destroy");
    }

    private class StatusHandler extends StatusAdapter {

        @Override
        public void onStatus(Status status) {
            Map<String, Object> tweetDetails = new HashMap<String, Object>();
            try {
                tweetDetails.put("id", status.getId());
                tweetDetails.put("status", JsonHelper.convertObjectToJson(status));
                tweetDetails.put("text", status.getText());
                tweetDetails.put("created_at", status.getCreatedAt());
                tweetDetails.put("source", status.getSource());
                tweetDetails.put("truncated", status.isTruncated());
                tweetDetails.put("lastEditDate", dateFormat.format(calendar.getTime()));
                if (status.getUserMentionEntities() != null) {
                    tweetDetails.put("mention", JsonHelper.convertObjectToJson(status.getUserMentionEntities()));
                }
                if (status.getRetweetCount() != -1) {
                    tweetDetails.put("retweet_count", status.getRetweetCount());
                }
                if (status.getHashtagEntities() != null) {
                    tweetDetails.put("hashtag", JsonHelper.convertObjectToJson(status.getHashtagEntities()));
                }
                if (status.getContributors() != null) {
                    tweetDetails.put("contributor", status.getContributors());
                }
                //builder.startObject("location");
                if (status.getGeoLocation() != null) {
                    tweetDetails.put("latitude", status.getGeoLocation().getLatitude());
                    tweetDetails.put("longitude", status.getGeoLocation().getLongitude());
                }
                //builder.startObject("place");
                if (status.getPlace() != null) {
                    tweetDetails.put("place", JsonHelper.convertObjectToJson(status.getPlace()));
                }
                if (status.getURLEntities() != null) {
                    List<Map<String, Object>> urlList = new ArrayList<Map<String, Object>>();
                    List<String> htmlList = new ArrayList<String>();

                    for (URLEntity url : status.getURLEntities()) {
                        Map<String, Object> urls = new HashMap<String, Object>();
                        if (url != null) {
                            if (url.getURL() != null) {
                                urls.put("url", url.getURL().toExternalForm());
                            }
                            if (url.getDisplayURL() != null) {
                                urls.put("display_url", url.getDisplayURL());
                            }
                            if (url.getExpandedURL() != null) {
                                urls.put("expandUrl", url.getExpandedURL());
                            }
                            urls.put("start", url.getStart());
                            urls.put("end", url.getEnd());

                            StringBuffer html = new StringBuffer();
                            try {
                                URLConnection conn = url.getURL().openConnection();
                                conn.setConnectTimeout(connectionTimeout);
                                conn.setReadTimeout(readTimeout);
                                InputStream in = conn.getInputStream();
                                String enc = null;
                                if (enc == null) {
                                    String cType = conn.getContentType();
                                    if (cType != null) {
                                        Matcher m = CHARSET_PATTERN.matcher(cType);
                                        if (m.find()) {
                                            enc = m.group(1);
                                        }
                                    }
                                }
                                if (enc == null)
                                    enc = "UTF-8";
                                BufferedReader br = new BufferedReader(new InputStreamReader(in, enc));
                                String nextLine = "";
                                while ((nextLine = br.readLine()) != null) {
                                    html.append(nextLine);
                                }

                            } catch (Exception e) {
                                logger.error(e);
                            }
                            urlList.add(urls);
                            htmlList.add(html.toString());
                        }
                    }
                    tweetDetails.put("link", urlList);
                    tweetDetails.put("html", htmlList);
                }


                //builder.startObject("user");

                tweetDetails.put("user", JsonHelper.convertObjectToJson(status.getUser()));

                tweetDetails.put("userID", status.getUser().getId());
                tweetDetails.put("userName", status.getUser().getName());
                tweetDetails.put("userScreenName", status.getUser().getScreenName());
                tweetDetails.put("userLocation", status.getUser().getLocation());
                tweetDetails.put("userDescription", status.getUser().getDescription());
                stackTweetDetails.push(tweetDetails);
            } catch (Exception e) {
                logger.warn("failed to construct index request", e);
            }

        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            if (statusDeletionNotice.getStatusId() != -1) {

            }
        }

        @Override
        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            logger.info("received track limitation notice, number_of_limited_statuses {}" + numberOfLimitedStatuses);
        }

        @Override
        public void onScrubGeo(long userId, long upToStatusId) {
            System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
        }

        @Override
        public void onException(Exception ex) {
            logger.warn("stream failure, restarting stream...", ex);
            reconnect();
        }

    }

    private class UserStreamListener implements twitter4j.UserStreamListener{
        @Override
        public void onStatus(Status status) {
            Map<String, Object> tweetDetails = new HashMap<String, Object>();

            try {


                tweetDetails.put("id", status.getId());
                tweetDetails.put("status", DataObjectFactory.getRawJSON(status));// skip StatusJSONImpl
                tweetDetails.put("text", status.getText());
                tweetDetails.put("created_at", status.getCreatedAt());
                tweetDetails.put("source", status.getSource());
                tweetDetails.put("truncated", status.isTruncated());
                if (status.getUserMentionEntities() != null) {

                    tweetDetails.put("mention", status.getUserMentionEntities());
                }

                if (status.getRetweetCount() != -1) {
                    tweetDetails.put("retweet_count", status.getRetweetCount());
                }


                if (status.getHashtagEntities() != null) {

                    tweetDetails.put("hashtag", status.getHashtagEntities());
                }
                if (status.getContributors() != null) {
                    tweetDetails.put("contributor", status.getContributors());
                }
                if (status.getGeoLocation() != null) {
                    //builder.startObject("location");
                    tweetDetails.put("latitude", status.getGeoLocation().getLatitude());
                    tweetDetails.put("longitude", status.getGeoLocation().getLongitude());

                }
                //builder.startObject("place");
                if (status.getPlace() != null) {

                    tweetDetails.put("place", status.getPlace());
                }
                if (status.getURLEntities() != null) {
                    List<Map<String, Object>> urlList = new ArrayList<Map<String, Object>>();
                    List<String> htmlList = new ArrayList<String>();

                    for (URLEntity url : status.getURLEntities()) {
                        Map<String, Object> urls = new HashMap<String, Object>();
                        if (url != null) {

                            if (url.getURL() != null) {
                                urls.put("url", url.getURL().toExternalForm());
                            }
                            if (url.getDisplayURL() != null) {
                                urls.put("display_url", url.getDisplayURL());
                            }
                            if (url.getExpandedURL() != null) {
                                urls.put("expandUrl", url.getExpandedURL());
                            }
                            urls.put("start", url.getStart());
                            urls.put("end", url.getEnd());

                            StringBuffer html = new StringBuffer();
                            try {
                                URLConnection conn = url.getURL().openConnection();
                                conn.setConnectTimeout(connectionTimeout);
                                conn.setReadTimeout(readTimeout);
                                InputStream in = conn.getInputStream();
                                String enc = null;
                                if (enc == null) {
                                    String cType = conn.getContentType();
                                    if (cType != null) {
                                        Matcher m = CHARSET_PATTERN.matcher(cType);
                                        if (m.find()) {
                                            enc = m.group(1);
                                        }
                                    }
                                }
                                if (enc == null)
                                    enc = "UTF-8";
                                BufferedReader br = new BufferedReader(new InputStreamReader(in, enc));


                                String nextLine = "";
                                while ((nextLine = br.readLine()) != null) {
                                    html.append(nextLine);
                                }

                            } catch (Exception e) {
                                logger.error(e);
                            }
                            urlList.add(urls);
                            htmlList.add(html.toString());
                        }
                    }
                    tweetDetails.put("link", urlList);
                    tweetDetails.put("html", htmlList);
                }


                //builder.startObject("user");
                tweetDetails.put("userID", status.getUser().getId());
                tweetDetails.put("userName", status.getUser().getName());
                tweetDetails.put("userScreenName", status.getUser().getScreenName());
                tweetDetails.put("userLocation", status.getUser().getLocation());
                tweetDetails.put("userDescription", status.getUser().getDescription());
                stackTweetDetails.push(tweetDetails);
            } catch (Exception e) {
                logger.warn("failed to construct index request", e);
            }

        }

        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
        }

        public void onDeletionNotice(long directMessageId, long userId) {
            System.out.println("Got a direct message deletion notice id:" + directMessageId);
        }

        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            System.out.println("Got a track limitation notice:" + numberOfLimitedStatuses);
        }

        public void onScrubGeo(long userId, long upToStatusId) {
            System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
        }

        public void onFriendList(long[] friendIds) {
            System.out.print("onFriendList");
            for (long friendId : friendIds) {
                System.out.print(" " + friendId);
            }
            System.out.println();
        }

        public void onFavorite(User source, User target, Status favoritedStatus) {
            System.out.println("onFavorite source:@"
                    + source.getScreenName() + " target:@"
                    + target.getScreenName() + " @"
                    + favoritedStatus.getUser().getScreenName() + " - "
                    + favoritedStatus.getText());
        }

        public void onUnfavorite(User source, User target, Status unfavoritedStatus) {
            System.out.println("onUnFavorite source:@"
                    + source.getScreenName() + " target:@"
                    + target.getScreenName() + " @"
                    + unfavoritedStatus.getUser().getScreenName()
                    + " - " + unfavoritedStatus.getText());
        }

        public void onFollow(User source, User followedUser) {
            System.out.println("onFollow source:@"
                    + source.getScreenName() + " target:@"
                    + followedUser.getScreenName());
        }

        public void onRetweet(User source, User target, Status retweetedStatus) {
            System.out.println("onRetweet @"
                    + retweetedStatus.getUser().getScreenName() + " - "
                    + retweetedStatus.getText());
        }

        public void onDirectMessage(DirectMessage directMessage) {
            System.out.println("onDirectMessage text:"
                    + directMessage.getText());
        }

        public void onUserListMemberAddition(User addedMember, User listOwner, UserList list) {
            System.out.println("onUserListMemberAddition added member:@"
                    + addedMember.getScreenName()
                    + " listOwner:@" + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        public void onUserListMemberDeletion(User deletedMember, User listOwner, UserList list) {
            System.out.println("onUserListMemberDeleted deleted member:@"
                    + deletedMember.getScreenName()
                    + " listOwner:@" + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        public void onUserListSubscription(User subscriber, User listOwner, UserList list) {
            System.out.println("onUserListSubscribed subscriber:@"
                    + subscriber.getScreenName()
                    + " listOwner:@" + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        public void onUserListUnsubscription(User subscriber, User listOwner, UserList list) {
            System.out.println("onUserListUnsubscribed subscriber:@"
                    + subscriber.getScreenName()
                    + " listOwner:@" + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        public void onUserListCreation(User listOwner, UserList list) {
            System.out.println("onUserListCreated  listOwner:@"
                    + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        public void onUserListUpdate(User listOwner, UserList list) {
            System.out.println("onUserListUpdated  listOwner:@"
                    + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        public void onUserListDeletion(User listOwner, UserList list) {
            System.out.println("onUserListDestroyed  listOwner:@"
                    + listOwner.getScreenName()
                    + " list:" + list.getName());
        }

        public void onUserProfileUpdate(User updatedUser) {
            System.out.println("onUserProfileUpdated user:@" + updatedUser.getScreenName());
        }

        public void onBlock(User source, User blockedUser) {
            System.out.println("onBlock source:@" + source.getScreenName()
                    + " target:@" + blockedUser.getScreenName());
        }

        public void onUnblock(User source, User unblockedUser) {
            System.out.println("onUnblock source:@" + source.getScreenName()
                    + " target:@" + unblockedUser.getScreenName());
        }

        public void onException(Exception ex) {
            logger.error("onException:" + ex.getMessage());
        }

    }
}
