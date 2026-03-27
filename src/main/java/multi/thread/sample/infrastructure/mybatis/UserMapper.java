package multi.thread.sample.infrastructure.mybatis;

import multi.thread.sample.batch.domain.UserBatchItem;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface UserMapper {

	void truncateUsers();

	void insertUser(UserBatchItem user);
}
