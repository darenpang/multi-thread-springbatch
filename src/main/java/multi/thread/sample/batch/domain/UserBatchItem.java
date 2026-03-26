package multi.thread.sample.batch.domain;

import java.time.LocalDate;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class UserBatchItem {

	String btn;
	Integer koza;
	String name;
	String kbn;
	LocalDate ymd;
}
