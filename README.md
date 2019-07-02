# Task description

**Problem**: SELECT statement for all the unique sessions with condition (given sequence of visited pages: page1, page2, page3) from table "pages" with columns: user_id, happened_at, page. Session length is 60 minutes.

**Solution 1**: Create session_id for each session of each user, then aggregate all the pages visited during same session into one line (new column "path"). Check sequence through this line. Optional: clean "path" column from excessive pages (page refreshing)

**Solution 2**:  Create session_id  for each session of each user, then join table to itself with condition (check given sequence of pages by time) 
