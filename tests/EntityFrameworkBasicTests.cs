#if NET40
using Npgsql;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.ComponentModel.DataAnnotations.Schema;

namespace NpgsqlTests
{
    [TestFixture]
    public class EntityFrameworkBasicTests : TestBase
    {
        public EntityFrameworkBasicTests(string backendVersion)
            : base(backendVersion)
        {
        }

        [TestFixtureSetUp]
        public override void TestFixtureSetup()
        {
            base.TestFixtureSetup();
            using (var context = new BloggingContext(ConnectionStringEF))
            {
                if (context.Database.Exists())
                    context.Database.Delete();//We delete to be 100% schema is synced
                context.Database.Create();
            }

            // Create sequence for the IntComputedValue property.
            using (var createSequenceConn = new NpgsqlConnection(ConnectionStringEF))
            {
                createSequenceConn.Open();
                ExecuteNonQuery("create sequence blog_int_computed_value_seq", createSequenceConn);
                ExecuteNonQuery("alter table \"dbo\".\"Blogs\" alter column \"IntComputedValue\" set default nextval('blog_int_computed_value_seq');", createSequenceConn);

            }
            

        }

        /// <summary>
        /// Clean any previous entites before our test
        /// </summary>
        [SetUp]
        protected override void SetUp()
        {
            base.SetUp();
            using (var context = new BloggingContext(ConnectionStringEF))
            {
                context.Blogs.RemoveRange(context.Blogs);
                context.Posts.RemoveRange(context.Posts);
                context.SaveChanges();
            }
        }

        public class Blog
        {
            public int BlogId { get; set; }
            public string Name { get; set; }

            public virtual List<Post> Posts { get; set; }

            [DatabaseGenerated(DatabaseGeneratedOption.Computed)]
            public int IntComputedValue { get; set; }
        }

        public class Post
        {
            public int PostId { get; set; }
            public string Title { get; set; }
            public string Content { get; set; }
            public byte Rating { get; set; }

            public int BlogId { get; set; }
            public virtual Blog Blog { get; set; }
        }

        public class BloggingContext : DbContext
        {
            public BloggingContext(string connection)
                : base(new NpgsqlConnection(connection), true)
            {
            }

            public DbSet<Blog> Blogs { get; set; }
            public DbSet<Post> Posts { get; set; }
        }

        [Test]
        public void InsertAndSelect()
        {
            using (var context = new BloggingContext(ConnectionStringEF))
            {
                var blog = new Blog()
                {
                    Name = "Some blog name"
                };
                blog.Posts = new List<Post>();
                for (int i = 0; i < 5; i++)
                    blog.Posts.Add(new Post()
                    {
                        Content = "Some post content " + i,
                        Rating = (byte)i,
                        Title = "Some post Title " + i
                    });
                context.Blogs.Add(blog);
                context.SaveChanges();
            }

            using (var context = new BloggingContext(ConnectionStringEF))
            {
                var posts = from p in context.Posts
                            select p;
                Assert.AreEqual(5, posts.Count());
                foreach (var post in posts)
                {
                    StringAssert.StartsWith("Some post Title ", post.Title);
                }
            }
        }

        [Test]
        public void SelectWithWhere()
        {
            using (var context = new BloggingContext(ConnectionStringEF))
            {
                var blog = new Blog()
                {
                    Name = "Some blog name"
                };
                blog.Posts = new List<Post>();
                for (int i = 0; i < 5; i++)
                    blog.Posts.Add(new Post()
                    {
                        Content = "Some post content " + i,
                        Rating = (byte)i,
                        Title = "Some post Title " + i
                    });
                context.Blogs.Add(blog);
                context.SaveChanges();
            }

            using (var context = new BloggingContext(ConnectionStringEF))
            {
                var posts = from p in context.Posts
                            where p.Rating < 3
                            select p;
                Assert.AreEqual(3, posts.Count());
                foreach (var post in posts)
                {
                    Assert.Less(post.Rating, 3);
                }
            }
        }

        [Test]
        public void OrderBy()
        {
            using (var context = new BloggingContext(ConnectionStringEF))
            {
                Random random = new Random();
                var blog = new Blog()
                {
                    Name = "Some blog name"
                };

                blog.Posts = new List<Post>();
                for (int i = 0; i < 10; i++)
                    blog.Posts.Add(new Post()
                    {
                        Content = "Some post content " + i,
                        Rating = (byte)random.Next(0, 255),
                        Title = "Some post Title " + i
                    });
                context.Blogs.Add(blog);
                context.SaveChanges();
            }

            using (var context = new BloggingContext(ConnectionStringEF))
            {
                var posts = from p in context.Posts
                            orderby p.Rating
                            select p;
                Assert.AreEqual(10, posts.Count());
                byte previousValue = 0;
                foreach (var post in posts)
                {
                    Assert.GreaterOrEqual(post.Rating, previousValue);
                    previousValue = post.Rating;
                }
            }
        }

        [Test]
        public void OrderByThenBy()
        {
            using (var context = new BloggingContext(ConnectionStringEF))
            {
                Random random = new Random();
                var blog = new Blog()
                {
                    Name = "Some blog name"
                };

                blog.Posts = new List<Post>();
                for (int i = 0; i < 10; i++)
                    blog.Posts.Add(new Post()
                    {
                        Content = "Some post content " + i,
                        Rating = (byte)random.Next(0, 255),
                        Title = "Some post Title " + (i % 3)
                    });
                context.Blogs.Add(blog);
                context.SaveChanges();
            }

            using (var context = new BloggingContext(ConnectionStringEF))
            {
                var posts = context.Posts.AsQueryable<Post>().OrderBy((p) => p.Title).ThenByDescending((p) => p.Rating);
                Assert.AreEqual(10, posts.Count());
                foreach (var post in posts)
                {
                    //TODO: Check outcome
                    Console.WriteLine(post.Title + " " + post.Rating);
                }
            }
        }

        [Test]
        public void TestComputedValue()
        {
            using (var context = new BloggingContext(ConnectionStringEF))
            {
                var blog = new Blog()
                {
                    Name = "Some blog name"
                };

                context.Blogs.Add(blog);
                context.SaveChanges();

                Assert.Greater(blog.BlogId, 0);
                Assert.Greater(blog.IntComputedValue, 0);
            }

        }

        private void BuildFullTextSearchTableTest()
        {
            // Create sequence for the IntComputedValue property.
            using (var createSequenceConn = new NpgsqlConnection(ConnectionStringEF))
            {
                createSequenceConn.Open();
                ExecuteNonQuery(@"  -- Table: posts
                                DROP TABLE IF EXISTS posts_fts cascade;

                                CREATE TABLE posts_fts
                                (
                                    id serial NOT NULL,
                                    title character varying,
                                    body character varying,
                                    user_name character varying,
                                    search_vector tsvector,
                                    CONSTRAINT posts_fts_pkey PRIMARY KEY (id)
                                )
                                WITH (
                                    OIDS=FALSE
                                );

                                -- Index: posts_search_idx

                                -- DROP INDEX posts_search_idx;

                                CREATE INDEX posts_fts_search_idx
                                ON posts_fts USING gin (search_vector);

                                -- Trigger: posts_vector_update on posts

                                -- DROP TRIGGER posts_vector_update ON posts;

                                CREATE TRIGGER posts_fts_vector_update
                                BEFORE INSERT OR UPDATE ON posts_fts
                                FOR EACH ROW
                                    EXECUTE PROCEDURE tsvector_update_trigger('search_vector', 'pg_catalog.english', 'title', 'body');
                            ", createSequenceConn);

                ExecuteNonQuery("INSERT INTO posts_fts (title, body, user_name) VALUES ('Postgres is awesome', '', 'Clark Kent')", createSequenceConn);
                ExecuteNonQuery("INSERT INTO posts_fts (title, body, user_name) VALUES ('How postgres is differente from MySQL', '', 'Lois Lane')", createSequenceConn);
                ExecuteNonQuery("INSERT INTO posts_fts (title, body, user_name) VALUES ('Tips for Mysql', '', 'Bruce Wayne')", createSequenceConn);
                ExecuteNonQuery("INSERT INTO posts_fts (title, body, user_name) VALUES ('SECRET', 'Postgres for the win', 'Dick Grayson')", createSequenceConn);
                ExecuteNonQuery("INSERT INTO posts_fts (title, body, user_name) VALUES ('Oracle acquires some other database', 'Mysql but no postgres' , 'Oliver Queen')", createSequenceConn);
                ExecuteNonQuery("INSERT INTO posts_fts (title, body, user_name) VALUES ('No Database', 'Nothing to see here', 'Kyle Ryner')", createSequenceConn);

            }

            
        }

        [Test]
        public void FullTextSearchSimpleTest()
        {
            BuildFullTextSearchTableTest();

            using (var ctx = new TestDbContext(ConnectionStringEF))
            {
                var query = @"select * 
                              from posts_fts 
                              where search_vector @@ to_tsquery('english', @p0) 
                              order by ts_rank_cd(search_vector, to_tsquery('english', @p0)) desc";
                var p = "postgres";
                var posts = ctx.Database.SqlQuery<Post>(query, p).ToList();

                Assert.AreEqual(4, posts.Count);
            }    
        }

        [Test]
        public void FullTextSearchAndTest()
        {
            BuildFullTextSearchTableTest();

            using (var ctx = new TestDbContext(ConnectionStringEF))
            {
                var query = @"select * 
                              from posts_fts 
                              where search_vector @@ to_tsquery('english', @p0) 
                              order by ts_rank_cd(search_vector, to_tsquery('english', @p0)) desc";
                var p = "postgres & mysql";
                var posts = ctx.Database.SqlQuery<Post>(query, p).ToList();

                Assert.AreEqual(2, posts.Count);
            }
        }

        [Test]
        public void FullTextSearchOrTest()
        {
            BuildFullTextSearchTableTest();

            using (var ctx = new TestDbContext(ConnectionStringEF))
            {
                var query = @"select * 
                              from posts_fts 
                              where search_vector @@ to_tsquery('english', @p0) 
                              order by ts_rank_cd(search_vector, to_tsquery('english', @p0)) desc";
                var p = "postgres | mysql";
                var posts = ctx.Database.SqlQuery<Post>(query, p).ToList();

                Assert.AreEqual(5, posts.Count);
            }
        }

        [Test]
        public void FirstOrDefaultTest()
        {
            BuildFullTextSearchTableTest();

            using (var ctx = new TestDbContext(ConnectionStringEF))
            {
                var post = ctx.Posts.FirstOrDefault();

                Assert.AreEqual("Clark Kent", post.UserName);
            }
        }


        public class TestDbContext : DbContext
        {
            public TestDbContext(string connection) : base(new NpgsqlConnection(connection), true)
            {
                Database.SetInitializer<TestDbContext>(null);
            }
            public DbSet<PostFts> Posts { get; set; }

            protected override void OnModelCreating(DbModelBuilder modelBuilder)
            {
                base.OnModelCreating(modelBuilder);
                modelBuilder.Entity<PostFts>().ToTable("posts_fts", "public");
                modelBuilder.Entity<PostFts>().Property(c => c.Id).HasColumnName("id").IsOptional();
                modelBuilder.Entity<PostFts>().Property(c => c.Title).HasColumnName("title").IsOptional();
                modelBuilder.Entity<PostFts>().Property(c => c.Body).HasColumnName("body").IsOptional();
                modelBuilder.Entity<PostFts>().Property(c => c.UserName).HasColumnName("user_name").IsOptional();
            }
        }

        public class PostFts
        {
            public int Id { get; set; }
            public string Title { get; set; }
            public string Body { get; set; }
            public string UserName { get; set; }
        }

    


        //Hunting season is open Happy hunting on OrderBy,GroupBy,Min,Max,Skip,Take,ThenBy... and all posible combinations
    }
}
#endif