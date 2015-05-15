package com.suny;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCancelOperationReq;
import org.apache.hive.service.cli.thrift.TCancelOperationResp;
import org.apache.hive.service.cli.thrift.TCloseOperationReq;
import org.apache.hive.service.cli.thrift.TCloseSessionReq;
import org.apache.hive.service.cli.thrift.TGetOperationStatusReq;
import org.apache.hive.service.cli.thrift.TGetOperationStatusResp;
import org.apache.hive.service.cli.thrift.TOpenSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;


public class cancle {
	private TOperationHandle stmtHandle;
	public static void cancel(String ip, String queryid) throws SQLException,
				IOException, ClassNotFoundException {

			TSocket transport = new TSocket(ip, 10000);

			TCLIService.Client client = new TCLIService.Client(new TBinaryProtocol(
					transport));
			try {
				transport.open();
				System.out.println("transport opened");

				TOpenSessionReq openReq = new TOpenSessionReq();
				System.out.println("    	TOpenSessionReq ");

				TOpenSessionResp openResp = client.OpenSession(openReq);
				System.out.println("    	TOpenSessionResp ");

				TSessionHandle sessHandle = openResp.getSessionHandle();
				System.out.println("    	TSessionHandle ");

				TCancelOperationReq cancelReq = new TCancelOperationReq();
				TOperationHandle stmtHandle = new TOperationHandle();

				// 从数据库中的记录构建请求参数对象↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
		
				
//				byte[] stmt = new byte[256];
//				BASE64Decoder dec = new BASE64Decoder();
//				try {
//					stmt = dec.decodeBuffer(rs);
//				} catch (IOException e1) {
//					e1.printStackTrace();
//				}
//				TDeserializer de = new TDeserializer();
//
//				System.out.println("stmtHandle " + stmtHandle);
//				de.deserialize(stmtHandle, stmt);
//				System.out.println("stmtHandle " + stmtHandle);
//				// 从数据库中的记录构建请求参数对象↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑
//
//				cancelReq.setOperationHandle(stmtHandle);
//				TCancelOperationResp cancelResp = client.CancelOperation(cancelReq);
//				System.out.println(cancelResp.getStatus());
//				
//				dao.deleteById(queryid);//取消hive任务之后删除mysql中的记录
//
//				TCloseOperationReq closeReq = new TCloseOperationReq();
//				closeReq.setOperationHandle(stmtHandle);
//				client.CloseOperation(closeReq);
//				TCloseSessionReq closeConnectionReq = new TCloseSessionReq(
//						sessHandle);
//				client.CloseSession(closeConnectionReq);
//
//				transport.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	
	public String getStatus() throws TException, SQLException, IOException {
		YarnConfiguration yarnConfig = new YarnConfiguration();
		String rmWebAddr = yarnConfig.get("yarn.resourcemanager.webapp.address");
		String resourceManagerIp = rmWebAddr.substring(0, rmWebAddr.lastIndexOf(":"));
		TSocket transport = new TSocket(resourceManagerIp, 10000);
		TCLIService.Client client = new TCLIService.Client(new TBinaryProtocol(transport));
		transport.open();
		
		TGetOperationStatusReq req = new TGetOperationStatusReq();
		req.setOperationHandle(this.stmtHandle);
		TGetOperationStatusResp statusResp = client.GetOperationStatus(req);
		System.out.println("status===="+statusResp.getOperationState().name());
		
		transport.close();
		return null;
	}

}

