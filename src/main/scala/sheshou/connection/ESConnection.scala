package sheshou.connection

import java.net.InetAddress

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient

/**
  * Created by wushiliang on 17/5/6.
  */
object ESConnection {

  def getConnection(): TransportClient ={

    val settings = Settings.builder.put("cluster.name", "elasticsearch").put("xpack.security.transport.ssl.enabled", false).
      put("xpack.security.user", "sheshou:sheshou12345").
      put("client.transport.sniff", true).build

    val ipAddr = Array[Byte](42, 123, 99, 38)
    val client = new PreBuiltXPackTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByAddress(ipAddr), 9300))

    return client
  }

}
