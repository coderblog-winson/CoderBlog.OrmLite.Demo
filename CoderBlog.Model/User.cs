using System.ComponentModel.DataAnnotations;
using ServiceStack.Model;

namespace CoderBlog.Model
{
    /// <summary>
    /// User model, and it's need to inherit IHasId<int> so that we can use the OrmLite code first 
    /// </summary>
    public class User : IHasId<int>
    {
        [ServiceStack.DataAnnotations.AutoIncrement]
        public int Id { get; set; }

        [Required]
        [StringLength(10)]
        public string UserName { get; set; }

        [Required]
        [StringLength(20)]
        public string Password { get; set; }

        [StringLength(30)]
        public string Email { get; set; }
               
        [StringLength(20)]
        public string FirstName { get; set; }

        [StringLength(20)]
        public string LastName { get; set; }
    }
}
