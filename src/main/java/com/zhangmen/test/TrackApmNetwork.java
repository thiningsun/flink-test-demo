package com.zhangmen.test;

import com.alibaba.excel.annotation.ExcelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author jinxing.zhang
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TrackApmNetwork {
    @ExcelProperty(value = "app_id", index = 0)
    private String appId;

    @ExcelProperty(value = "user_id", index = 1)
    private String userId;

    @ExcelProperty(value = "device_id", index = 2)
    private String deviceId;

    @ExcelProperty(value = "session_id", index = 3)
    private String sessionId;

    @ExcelProperty(value = "lesson_uid", index = 4)
    private String lessonUid;

    @ExcelProperty(value = "tracker_type", index = 5)
    private Integer trackerType;

    @ExcelProperty(value = "time", index = 6)
    private Long time;



    @ExcelProperty(value = "c_source", index = 7)
    private String cSource;

    @ExcelProperty(value = "counter", index = 8)
    private Integer counter;

    @ExcelProperty(value = "scene", index = 9)
    private Integer scene;

    @ExcelProperty(value = "client_trans_id", index = 10)
    private String clientTransId;

    @ExcelProperty(value = "server_time", index = 11)
    private Long serverTime;

    @ExcelProperty(value = "ip", index = 12)
    private String ip;

    @ExcelProperty(value = "method", index = 13)
    private String method;

    @ExcelProperty(value = "protocol", index = 14)
    private String protocol;

    @ExcelProperty(value = "local_queue_time", index = 15)
    private Integer localQueueTime;

    @ExcelProperty(value = "bytes_sent", index = 16)
    private Long bytesSent;

    @ExcelProperty(value = "url", index = 17)
    private String url;

    @ExcelProperty(value = "duration", index = 18)
    private Integer duration;

    @ExcelProperty(value = "bytes_received", index = 19)
    private Long bytesReceived;

    @ExcelProperty(value = "dns_time", index = 20)
    private Integer dnsTime;

    @ExcelProperty(value = "remain_package_time", index = 21)
    private Integer remainPackageTime;

    @ExcelProperty(value = "first_packet_time", index = 22)
    private Integer firstPacketTime;

    @ExcelProperty(value = "ssl_time", index = 23)
    private Integer sslTime;

    @ExcelProperty(value = "application_data", index = 24)
    private String applicationData;

    @ExcelProperty(value = "http_lib_type", index = 25)
    private String httpLibType;

    @ExcelProperty(value = "http_status_code", index = 26)
    private Integer httpStatusCode;

    @ExcelProperty(value = "error_message", index = 27)
    private String errorMessage;

    @ExcelProperty(value = "client_time", index = 28)
    private String clientTime;

}
