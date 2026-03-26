package multi.thread.sample.infrastructure.mybatis;

import java.util.List;
import multi.thread.sample.batch.domain.UserBatchItem;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface UserMapper {

	void truncateUsers();

	void insertUsers(@Param("users") List<UserBatchItem> users);
}
