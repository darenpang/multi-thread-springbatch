package multi.thread.sample.batch.processor;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import multi.thread.sample.batch.csv.UserCsvRecord;
import multi.thread.sample.batch.domain.UserBatchItem;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class UserItemProcessor implements ItemProcessor<UserCsvRecord, UserBatchItem> {

	private static final DateTimeFormatter BASIC_DATE = DateTimeFormatter.BASIC_ISO_DATE;

	@Override
	public UserBatchItem process(UserCsvRecord item) {
		var normalizedBtn = item.btn() == null ? null : "%4s".formatted(item.btn().trim());
		var normalizedKbn = item.kbn() == null ? null : item.kbn().trim();
		var normalizedYmd = item.ymd() == null ? null : item.ymd().trim();

		return UserBatchItem.builder()
				.btn(normalizedBtn)
				.koza(item.koza())
				.name(item.name())
				.kbn(normalizedKbn)
				.ymd(LocalDate.parse(normalizedYmd, BASIC_DATE))
				.build();
	}
}
