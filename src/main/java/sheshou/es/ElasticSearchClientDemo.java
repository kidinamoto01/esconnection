package sheshou.es;

/**
 * Created by wushiliang on 17/5/5.
 */
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

/**
 *
 * @author Administrator
 * @version $Id: ElasticSearchClientDemo.java, v 0.1 2017年4月14日 上午8:37:38 Administrator Exp $
 */
public class ElasticSearchClientDemo {

    /**new PreBuiltXPackTransportClient(Settings.builder()
     *
     * @param args
     * @throws UnknownHostException
     */
    public static void main(String[] args) throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").put("xpack.security.user", "sheshou:sheshou12345").put("client.transport.sniff", true).build();

        TransportClient client = new PreBuiltXPackTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("42.123.99.38"), 9300));

        //        GetResponse response = client.prepareGet("zq1", "first", "5d0b0c8bc042aca1a969b70d2e7dd942").execute().actionGet();
        //        System.out.println(response.getSourceAsString());

        SearchResponse searchResp = client.prepareSearch("sheshou_info").setTypes("first").setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(QueryBuilders.multiMatchQuery("华夏创新科技", "_all")) // Query
                .setPostFilter(QueryBuilders.rangeQuery("pubdate").from("2016-02-12").to("2016-02-15")) // Filter
                .setFrom(0).setSize(60).setExplain(true).get();

        SearchHits hits = searchResp.getHits();
        System.out.println(hits.getTotalHits());
        for (SearchHit hit : hits.getHits()) {
            System.out.println(hit.getSource());
        }
        client.close();
    }

}
